import geode.utils as utils
from geode.event import Event
import copy
import datetime
import logging
import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.extras import RealDictCursor


class Database:
    """All of the functionality for interacting with the backend database"""
    def __init__(self,table_option,
                 config_file='/etc/geode/justin_test_settings.conf',
                 log_file='/var/log/geode/justin_test_geode.log'):
        """Create a new connection to the database"""

        __tables=utils.get_tables(0)
        # Turn on logging
        logging.basicConfig(filename=log_file, level=logging.INFO)

        # Connect to the database
        self.database, self.cursor = self._connect()
   
        # Turn on autocommit because it's nice to have
        self.database.autocommit = True
        if(table_option is not "create_tables"):
            __table=__tables[__tables.index(table_option)]
            if __table!="sediment":
                print(__table)
    
        if table_option is not "create_tables" and __table!="sediment":
            self.cursor.execute("DELETE FROM "+__table)
        # Create our prepared statements
    
        if(table_option is not "create_tables"):
            self.cursor.execute(
                """PREPARE insert_plan(macaddr, inet, text, text, timestamp,
                                     timestamp, text, text, smallint[])
                 AS
                 INSERT INTO """ + __table+""" (mac, ip, netid, hostname, start,
                                           stop, useragent, os, event_type)
                 VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9);""")
            self.cursor.execute("""PREPARE select_id_plan(bigint) AS
                                   SELECT * FROM """+__table+""" WHERE id=$1;""")
            
            self.cursor.execute(
                """PREPARE select_mac_ip_plan(macaddr, inet, timestamp, timestamp)
                   AS SELECT * FROM """+__table+
                   """ WHERE mac=$1 AND ip=$2 AND (
                       ($3 <= start AND start <=$4) OR
                       ($3 <= stop AND stop <= $4) OR
                       (start <= $3 AND $4 <= stop)) ORDER BY start DESC LIMIT 1;""")

            self.cursor.execute(
                """PREPARE select_mac_plan(macaddr, timestamp, timestamp)
                   AS SELECT * FROM """+__table+
                   """ WHERE mac=$1 AND (
                       ($2 <= start AND start <=$3) OR
                       ($2 <= stop AND stop <= $3) OR
                       (start <= $2 AND $3 <= stop)) ORDER BY start DESC LIMIT 1;""")

            self.cursor.execute(
                """PREPARE update_plan (macaddr, inet, text, text, timestamp,
                                        timestamp, text, text, smallint[])
                   AS UPDATE """+__table+""" SET mac=$1, ip=$2, netid=$3,
                                               hostname=$4, start=$5, stop=$6,
                                               useragent=$7, os=$8, event_type=$9
                   WHERE id=$10;""")
            
            self.cursor.execute(
                """PREPARE update_netid_hostname_plan (text, text)
                   AS UPDATE """+__table+""" SET netid=$1, hostname=$2 WHERE id=$3;""")
            
            self.cursor.execute(
                """PREPARE update_netid_plan (text)
                   AS UPDATE """+__table+""" SET netid=$1 WHERE id=$2;""")
            
            self.cursor.execute(
                """PREPARE update_hostname_plan (text)
                   AS UPDATE """+__table+""" SET hostname=$1 WHERE id=$2;""")
            
            self.cursor.execute(
                """PREPARE update_start_plan (timestamp)
                   AS UPDATE """+__table+""" SET start=$1 WHERE id=$2;""")

            self.cursor.execute(
                """PREPARE update_stop_plan (timestamp)
                   AS UPDATE """+__table+""" SET stop=$1 WHERE id=$2;""")


            self.cursor.execute(
                """PREPARE select_ip_plan(inet, timestamp, timestamp)
                   AS SELECT * FROM """+__table+
                   """ WHERE ip=$1 AND (
                       ($2 <= start AND start <=$3) OR
                       ($2 <= stop AND stop <= $3) OR
                       (start <= $2 AND $3 <= stop)) ORDER BY start DESC LIMIT 1;""")

            self.cursor.execute(
                """PREPARE select_mac_plan_info(macaddr, timestamp)
                   AS SELECT * FROM """+__table+
                   """ WHERE mac=$1 AND (
                       (start <= $2 AND $2 <= stop)) ORDER BY start DESC LIMIT 1;""")
            self.cursor.execute(
                """PREPARE select_ip_plan_info(inet, timestamp)
                   AS SELECT * FROM """+__table+
                   """ WHERE ip=$1 AND (
                       (start <= $2 AND $2 <= stop)) ORDER BY start DESC LIMIT 1;""")



    def _create_debug_tables(self):
        __debug_tables=utils.get_tables(1)
        for _table in __debug_tables:
            self.cursor.execute("DROP TABLE IF EXISTS "+_table+";")
            self.cursor.execute("DROP SEQUENCE IF EXISTS "+_table+"_seq;")
            print("creating table "+_table)
            self.cursor.execute("CREATE SEQUENCE IF NOT EXISTS "+_table+"_seq;")
            self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS "+_table+""" (
            id bigint NOT NULL default nextval('"""+_table+"""_seq'::regclass),
            mac macaddr,
            ip inet,
            netid text,
            hostname text,
            start timestamp(2) without time zone,
            stop timestamp(2) without time zone,
            useragent text,
            os text,
            event_type smallint[]);
            """)
        self.cursor.execute("DROP TABLE IF EXISTS table_info;");
        to_be_executed="CREATE TABLE IF NOT EXISTS table_info ("
        for i in range(0,len(__debug_tables)-1):
            to_be_executed=to_be_executed+__debug_tables[i]+" text, "
        to_be_executed=to_be_executed+__debug_tables[len(__debug_tables)-1]+" text);"
        self.cursor.execute(to_be_executed)
	
    def _connect(self):
        """Connect to the database and return the connection and cursor"""
        
        section = "database"
        username = utils.read_config(section, "username")
        password = utils.read_config(section, "password", raw=True)
        host = utils.read_config(section, "host")
        database = utils.read_config(section, "database")
       
        try:
            conn = psycopg2.connect(user=username,
                                    password=password,
                                    host=host,
                                    database=database,
                                    cursor_factory=RealDictCursor)
      
            return conn, conn.cursor()
        except Exception as e:
            print("db connection didnt work")
            logging.exception('Postgres connection failure: {0}'.format(str(e)))
            raise e
     
    def insert(self, event):
        """Inserts a new event into the database

        Returns True on success
        """

        # NOTE: This should never happen because of the check in event.__init__
        if not 'start' and 'stop' in event.keys():
            raise Exception("No start or stop time for event")

        # We will replace the strings in the event_type with the corresponding
        # ints, but we want them to stay strings in the object itself, so we
        # just replace them later on when we're done
        tmp = None

        # Convert the event_type strings into their corresponding ints
        if event.get('event_type'):
            tmp = [Event.types.get(x) for x in event.get('event_type')]
        # The values to be inserted into the database
        values = tuple([event[key] if type(event[key]) is not datetime.datetime
                       else utils.dto_to_string(event[key])
                       for key in event.keys()])

        # Query to be executed
        #TODO: Oh man, this is so janky
        query = """EXECUTE insert_plan(%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        data = [
                event.get('mac'),
                event.get('ip'),
                event.get('netid'),
                event.get('hostname'),
                utils.dto_to_string(event.get('start')),
                utils.dto_to_string(event.get('stop')),
                event.get('useragent'),
                event.get('os'),
                tmp  # The event type list
               ]

        # TODO: Do conversion between event text and event number
        self.cursor.execute(query, data)

        return True
    
    def select_dhcp(self, event, DEFAULT_BUFFER):
        """Selects the data from the database that matches the event dhcp style"""

        if (event.get('mac') is (None or '')) and (event.get('ip') is (None or '')):
            print("ERROR! DHCP RETURNED A MAC OR AN IP BUT NOT BOTH, OR RETURNED NEITHER!!")
            raise Exception("Not enough data to select upon: Mac/IP required")

        normal_start=event.get('start')
        if type(normal_start) is str:
            adjusted_start = utils.time_diff_string(normal_start,-DEFAULT_BUFFER)
        else:
            adjusted_start = utils.time_diff(normal_start,-DEFAULT_BUFFER)

        sql = """EXECUTE select_mac_plan(%s, %s, %s);"""
        data = (event.get('mac'), adjusted_start, event.get('stop'))
        self.cursor.execute(sql, data)
        results = self.cursor.fetchall()
        if len(results) == 0:
            return None
        else:
            return Event(results[-1]) 
    def select_info(self, event):
        """Selects the data from the database that matches the event info style"""

        normal_start=event.get('start')
        mac=event.get('mac')
        if("wireless_authentication" in event.get('event_type')):#there is wireless auth
            LATENCY_BUFFER=600#this is in seconds
            if type(normal_start) is str:
                adjusted_stop = utils.time_diff_string(normal_start,+LATENCY_BUFFER)
                ##NOTE: using normal start instead of stop is not a typo.
                ##      we treat info as blips/moments in time and ignore stop
                ##      so when we account for latency, we look forward 30s of start
                ##      not stop, because stop could be anything
            else:
                adjusted_stop = utils.time_diff(normal_start,+LATENCY_BUFFER)
            if mac is not None:
                sql = """EXECUTE select_mac_plan(%s, %s, %s);"""
                data=(mac, normal_start, adjusted_stop)
            else:
                sql = """EXECUTE select_ip_plan(%s, %s, %s);"""
                data=(event.get('ip'), normal_start, adjusted_stop)
            self.cursor.execute(sql, data)
            results=self.cursor.fetchall()
        else:#info contains no wireless auth
            if mac is not None and (mac is not ''):
                sql = """EXECUTE select_mac_plan_info(%s, %s);"""
                data=(mac, normal_start)
            elif event.get('ip') is not None:
                sql = """EXECUTE select_ip_plan_info(%s, %s);"""
                data=(event.get('ip'), normal_start)
            else:
                raise Exception("Not enough data to select upon: Mac/IP required")
            self.cursor.execute(sql, data)
            results = self.cursor.fetchall()
            if len(results)==0:
                self.cursor.execute("SELECT id FROM data_to_check_one_min WHERE ip='"+str(event.get('ip'))+"';")
                stuff=self.cursor.fetchall()
                if(len(stuff)>0):
                    print("event that didn't map start=")
                    print(event.get('start'))
                    eve=event.get('event_type')
                    for ev in eve:
                        print(ev)
                    print("id's=: ")
                    for key in (stuff):
                        print(key)
                    print("end of that one")
        if len(results)==0:
            return None
        else:
            return Event(results[-1])   

    def select_mac_ip(self, event, DEFAULT_BUFFER):
        """Selects the data from the database that matches the event"""
        print("we replaced this, where is it running???")
        # If we have an ID, then let's select based on that
        if event.get('id') is not None:
            print("this probably should never happen!!!! I'm in databse.py")
            sql = """EXECUTE select_id_plan(%s);"""
            data = (event.get('id'), )
            self.cursor.execute(sql, data)

            return Event(self.cursor.fetchall()[0])

        fields = ()
        values = ()

        if event.get('mac') is not None:
            fields += ('mac',)
            values += (event.get('mac'),)

        if event.get('ip') is not None:
            fields += ('ip',)
            values += (event.get('ip'),)

        # We need at least a MAC or an IP in order to select anything
        if not fields:
            print("problem in database.py")
            raise Exception("Not enough data to select upon: Mac/IP required")
        
        normal_start=event.get('start')
        normal_stop=event.get('stop')
        
        # If we have an MAC address and an IP address, check either



        if type(normal_start) is str:
            adjusted_start = utils.time_diff_string(normal_start,-DEFAULT_BUFFER)
        else:
            adjusted_start = utils.time_diff(normal_start,-DEFAULT_BUFFER)
        if type(normal_stop) is str:
            adjusted_stop = utils.time_diff_string(normal_stop, DEFAULT_BUFFER)
        else:
            adjusted_stop = utils.time_diff(normal_stop, DEFAULT_BUFFER)
        
        
        
        if "mac" in fields:
            sql = """EXECUTE select_mac_plan(%s, %s, %s);"""
            data = (values[0], adjusted_start, adjusted_stop)
        # Otherwise we have an IP
        else:
            sql = """EXECUTE select_ip_plan(%s, %s, %s);"""
            data = (values[0], adjusted_start, adjusted_stop)

        self.cursor.execute(sql, data)
        results = self.cursor.fetchall()
        if len(results) != 0:
            if "mac" in fields:
                sql = """EXECUTE select_mac_plan(%s, %s, %s);"""
                data = (values[0], normal_start, normal_stop)
            # Otherwise we have an IP
            else:
                sql = """EXECUTE select_ip_plan(%s, %s, %s);"""
                data = (values[0], normal_start, normal_stop)

            self.cursor.execute(sql, data)
            results2=self.cursor.fetchall()
            if len(results2) !=0:
                results=results2

        # If we didn't find anything, then account for latency. We don't do this at first because if it's better to be precise
            if len(results)==0:
                return None
            else:
                return Event(results[-1])#return the latest result because this will contain the most recent data, we do this because there could be terminated data from before that we no longer want or other outdated data
        
        #used to be:
        #e = Event(results[0])
        #return Event(results[0]) if e.matches(event) else None other changes include no more matching and only allow mac ip matching from sql queries to better allow termination

    def update(self, event, event_id):
        """Updates the SQL event with the given ID to contain the event values

        All of the values of the SQL entry will be replaced with the values
        from the event. This means that the merged event should be passed in
        to this function.

        Returns True on success
        """

        # NOTE: This should never happen because of the check in event.__init__
        if not 'start' and 'stop' in event.keys():
            raise Exception("No start or stop time for event")

        # Query to be executed
        query = """EXECUTE update_plan(%s, %s, %s, %s, %s,
                                       %s, %s, %s, %s, %s);"""
        tmp = [Event.types.get(x) for x in event.get('event_type')]

        data = [event.get('mac'),
                event.get('ip'),
                event.get('netid'),
                event.get('hostname'),
                utils.dto_to_string(event.get('start')),
                utils.dto_to_string(event.get('stop')),
                event.get('useragent'),
                event.get('os'),
                tmp,
                event_id]
        self.cursor.execute(query, data)

        return True

    def update_start_time(self,new_time, event_id):
        """Replaces the SQL event with the given ID's time to the new time.

        Returns True on success
        """
        # Query to be executed
        query = """EXECUTE update_start_plan(%s, %s);"""
        data = [new_time,
                event_id]
        self.cursor.execute(query, data)

        return True

    def update_stop_time(self,new_time, event_id):
        """Replaces the SQL event with the given ID's time to the new time.

        Returns True on success
        """
        # Query to be executed
        query = """EXECUTE update_stop_plan(%s, %s);"""
        data = [new_time,
                event_id]
        self.cursor.execute(query, data)
        return True

    def update_netid_hostname(self,new_netid,new_hostname,event_id):
        """Replaces the SQL event with the given ID's netid and hostname.

        Returns True on success
        """
        # Query to be executed
        if (new_netid is not None) and (new_hostname is not None):
            query = """EXECUTE update_netid_hostname_plan(%s, %s, %s);"""
            data = [new_netid,new_hostname,event_id]
        elif new_netid is not None:
            query = """EXECUTE update_netid_plan(%s, %s);"""
            data = [new_netid,event_id]
        else:
            query = """EXECUTE update_hostname_plan(%s, %s);"""
            data = [new_hostname,event_id]
        self.cursor.execute(query, data)
        return True

    def terminate(self, event, time):
        #########INCORRECT: look at terminate_new
        """Sets the stop time in the database to be the specified time

        Returns True on success
        """

        event_id = event.get('id')

        if not event_id:
            raise Exception("No id in given event")

        # Set the stop time of the event
        event['stop'] = time
        self.update(event, event_id)

        return True
    def terminate_new(self, old_event, new_event):
        """takes the old event and deep copies it into b
           sets old_event.stop=new event start
           updates old event
           sets b.start=new event start
           merges b and new event
           inserts b
           
        """
        #####NOT OPTIMIZED. look at terminate_info and dhcp, merge is slow this is better
        tmp_second_half = copy.deepcopy(old_event)
        self.update_stop_time(new_event.get('start'),old_event.get('id'))
        tmp_second_half['start']=new_event.get('start')
        to_insert=tmp_second_half.merge(new_event)
        self.insert(to_insert)
        return True
    
    def terminate_info(self, old_event, new_event):
        """takes the old event and deep copies it into b
           sets old_event.stop=new event start
           updates old event
           sets b.start=new event start
           merges b and new event
           inserts b
           
        """
        ###NOT OPTIMIZED: instead of doing the copy here, copy inside of the database with the new values
        tmp_second_half = copy.deepcopy(old_event)
        self.update_stop_time(new_event.get('start'),old_event.get('id'))
        tmp_second_half['start']=new_event.get('start')
        if new_event.get('netid') is not None:
            tmp_second_half['netid']=new_event.get('netid')
        if new_event.get('hostname') is not None:
            tmp_second_half['hostname']=new_event.get('hostname')
        self.insert(tmp_second_half)
        return True
    
    def terminate_DHCP(self, old_event, new_event):
        """takes the old event and deep copies it into b
           sets old_event.stop=new event start
           updates old event
           sets b.start=new event start
           merges b and new event
           inserts b
           
        """
        ###NOT OPTIMIZED: instead of doing the copy here do it in the database with new values
        tmp_second_half = copy.deepcopy(old_event)
        self.update_stop_time(new_event.get('start'),old_event.get('id'))
        tmp_second_half['start']=new_event.get('start')
        tmp_second_half['stop']=new_event.get('stop')
        tmp_second_half['ip']=new_event.get('ip')
        self.insert(tmp_second_half)
        return True

