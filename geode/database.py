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
            logging.error('Postgres connection failure: {0}'.format(str(e)))
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
        tmp = event.get('event_type')

        # Convert the event_type strings into their corresponding ints
        if event.get('event_type'):
            event['event_type'] = [Event.types.get(x) for x in
                                    event.get('event_type')]
        # The values to be inserted into the database
        values = tuple([event[key] if type(event[key]) is not datetime.datetime
                       else utils.dto_to_string(event[key])
                       for key in event.keys()])

        # Query to be executed
        #TODO: Oh man, this is so janky
        query = """INSERT INTO sediment (%s) VALUES (""" + ("%s, "*len(values))[:-2] + ");"
        data = [AsIs(','.join(event.keys()))]
        data.extend(values)
        # We need to do things differently depending on if there is only
        # one key or if there are multiple keys

        # TODO: Do conversion between event text and event number
        self.cursor.execute(query, data)

        event['event_type'] = tmp

        return True

    def select(self, event):
        """Selects the data from the database that matches the event"""

        # If we have an ID, then let's select based on that
        if event.get('id') is not None:
            sql = """SELECT * FROM sediment WHERE id=(%s);"""
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
        if type(event.get('start')) is str:
            adjusted_stop = utils.time_diff_string(event.get('stop'), 30)
        else:
            adjusted_stop = utils.time_diff(event.get('stop'), 30)

        # If we have an MAC address and an IP address, check either
        # TODO: Wow, this code looks bad
        if "mac" or "ip" not in fields:
            sql = """SELECT * FROM sediment
                     WHERE mac = (%s) OR ip = (%s)
                     AND (
                          ((%s) <= start AND start <= (%s)) OR
                          ((%s) <= stop AND stop <= (%s)) OR
                          (start <= (%s) AND (%s) <= stop)
                         )
                     ORDER BY stop DESC, id DESC
                     LIMIT 1;"""
            data = (values[0], values[1],
                    adjusted_start, adjusted_stop,
                    adjusted_start, adjusted_stop,
                    adjusted_start, adjusted_start)
        else:
            sql = """SELECT * FROM sediment
                     WHERE (%s) = ('%s')
                     AND (
                          ((%s) <= start AND start <= (%s)) OR
                          ((%s) <= stop AND stop <= (%s)) OR
                          (start <= (%s) AND (%s) <= stop)
                         )
                     ORDER BY stop DESC, id DESC
                     LIMIT 1;"""
            data = (AsIs(fields[0]), AsIs(values[0]),
                    adjusted_start, adjusted_stop,
                    adjusted_start, adjusted_stop,
                    adjusted_start, adjusted_start)

        self.cursor.execute(sql, data)
        results = self.cursor.fetchall()

        # If we didn't find anything, return None
        if len(results) == 0:
            print("Nothing found")
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

        # The values to be inserted into the database
        values = []
        for key in event.keys():
            if key == 'event_type':
                values.append(list([Event.types.get(x) for x in
                               event.get('event_type')]))
            elif key == 'id':
                continue
            elif isinstance(event[key], datetime.datetime):
                values.append(utils.dto_to_string(event[key]))
            else:
                values.append(event[key])
        values = tuple(values)

        # Query to be executed
        query = """UPDATE sediment SET (%s) = (""" + ("%s, "*len(values))[:-2] + ") WHERE id = %s;"
        data = [AsIs(','.join([x for x in event.keys() if x != 'id']))]
        data.extend(values)
        data.append(event.get('id'))
        self.cursor.execute(query, data)

        return True

    def terminate(self, event, time):
        """Sets the stop time in the database to be the specified time

        Returns True on success
        """

        event_id = event.get('id')

        if not event_id:
            raise Exception("No id in given event")

        # Convert the date time object to be a string in the given format
        time = utils.dto_to_string(time)
        # Set the stop time of the event
        event['stop'] = time

        # Do the update
        self.udpate(event, event_id)

        return True
