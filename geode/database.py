import geode.utils as utils
from geode.event import Event

import datetime
import logging
import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.extras import RealDictCursor


class Database:
    """All of the functionality for interacting with the backend database"""

    def __init__(self,
                 config_file='/etc/geode/settings.conf',
                 log_file='/var/log/geode/geode.log'):
        """Create a new connection to the database"""

        # Turn on logging
        logging.basicConfig(filename=log_file, level=logging.INFO)

        # Connect to the database
        self.database, self.cursor = self._connect()

        # Turn on autocommit because it's nice to have
        self.database.autocommit = True

        # Create our prepared statements
        self.cursor.execute(
            """PREPARE insert_plan(macaddr, inet, text, text, timestamp,
                                 timestamp, text, text, smallint[])
             AS
             INSERT INTO sediment(mac, ip, netid, hostname, start,
                                       stop, useragent, os, event_type)
             VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9);""")

        self.cursor.execute("""PREPARE select_id_plan(bigint) AS
                               SELECT * FROM sediment WHERE id=$1;""")

        self.cursor.execute(
            """PREPARE select_mac_plan(macaddr, timestamp, timestamp)
               AS SELECT * FROM sediment
               WHERE mac=$1 AND (
                   ($2 <= start AND start <=$3) OR
                   ($2 <= stop AND stop <= $3) OR
                   (start <= $2 AND $2 <= stop));""")

        self.cursor.execute(
            """PREPARE update_plan (macaddr, inet, text, text, timestamp,
                                    timestamp, text, text, smallint[])
               AS UPDATE sediment SET mac=$1, ip=$2, netid=$3,
                                           hostname=$4, start=$5, stop=$6,
                                           useragent=$7, os=$8, event_type=$9
               WHERE id=$10;""")

        self.cursor.execute(
            """PREPARE select_ip_plan(inet, timestamp, timestamp)
               AS SELECT * FROM sediment
               WHERE ip=$1 AND (
                   ($2 <= start AND start <=$3) OR
                   ($2 <= stop AND stop <= $3) OR
                   (start <= $2 AND $2 <= stop));""")

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

    def select(self, event):
        """Selects the data from the database that matches the event"""

        # If we have an ID, then let's select based on that
        if event.get('id') is not None:
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
            raise Exception("Not enough data to select upon: Mac/IP required")

        # Give us a 30 second buffer for start and stop
        if type(event.get('start')) is str:
            adjusted_start = utils.time_diff_string(event.get('start'), 30)
        else:
            adjusted_start = utils.time_diff(event.get('start'), 30)
        if type(event.get('stop')) is str:
            adjusted_stop = utils.time_diff_string(event.get('stop'), 30)
        else:
            adjusted_stop = utils.time_diff(event.get('stop'), 30)

        # If we have an MAC address and an IP address, check either
        if "mac" in fields:
            sql = """EXECUTE select_mac_plan(%s, %s, %s);"""
            data = (values[0], adjusted_start, adjusted_stop)
        # Otherwise we have an IP
        else:
            sql = """EXECUTE select_ip_plan(%s, %s, %s);"""
            data = (values[0], adjusted_start, adjusted_stop)

        self.cursor.execute(sql, data)
        results = self.cursor.fetchall()

        # If we didn't find anything, return None
        if len(results) == 0:
            return None

        # TODO: For now, we are only checking the first event, is this okay?
        e = Event(results[0])
        return Event(results[0]) if e.matches(event) else None

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

    def terminate(self, event, time):
        """Sets the stop time in the database to be the specified time

        Returns True on success
        """

        event_id = event.get('id')

        if not event_id:
            raise Exception("No id in given event")

        # Set the stop time of the event
        event['stop'] = time

        # Do the update
        self.update(event, event_id)

        return True
