import geode.utils as utils
from geode.event import Event

import logging
import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.extras import RealDictCursor


class Database:
    """All of the functionality for interacting with the backend database"""

    def __init__(self,
                 config_file='/etc/geode/test_settings.conf',
                 log_file='/var/log/geode/geode.log'):
        """Create a new connection to the database"""

        # Turn on logging
        logging.basicConfig(filename=log_file, level=logging.DEBUG)

        # Connect to the database
        self.database, self.cursor = self._connect()

        # Turn on autocommit because it's nice to have
        self.database.autocommit = True

    def _connect(self):
        """Connect to the database and return the connection and cursor"""

        section = "database"
        username = utils.read_config(section, "username")
        password = utils.read_config(section, "password")
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

    def insert(self, event, table):
        """Inserts a new event into the database"""

        # The values to be inserted into the database
        values = tuple([event[key] for key in event.keys()])
        # Query to be executed
        query = """INSERT INTO %s (%s) VALUES ('%s');"""
        # We need to do things differently depending on if there is only
        # one key or if there are multiple keys

        # TODO: Do conversion between event text and event number

        if len(event.keys()) > 1:
            # if there is more than one key, then we need to build up a string
            # that is separated by commas for the keys (columns), and the
            # values can just be the tuple
            self.cursor.execute(query,
                                AsIs(table),
                                AsIs(','.join(event.keys())),
                                AsIs(values))
        elif len(event.keys()) > 0:
            # if we only have one key, then we need to just take the first
            # element from the values variable and make sure we quote it,
            # and surround it with parenthesis
            query = """INSERT INTO %s (%s) VALUES ('%s');"""
            self.cursor.execute(query,
                                AsIs(table),
                                AsIs(event.keys()[0]),
                                AsIs(values[0]))

    def select(self, event, table):
        """Selects the data from the database that matches the event"""

        # If we have an ID, then let's select based on that
        if event.get('id') is not None:
            sql = """SELECT * FROM %s WHERE id=(%s);"""
            data = (AsIs(table), event.get('id'))
            self.cursor.execute(sql, data)

            return self.cursor.fetchall()

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
        adjusted_start = utils.time_diff(event.get('start'), 30)
        adjusted_stop = utils.time_diff(event.get('stop'), 30)

        # If we have an MAC address and an IP address, check either
        # TODO: Wow, this code looks bad
        if len(values) == 2:
            """
            There are 4 forms of overlapping for events
                 |----db---|
            1)     |--event--|
            2) |----event----|
            3)         |----event----|
            4) |--------event--------|
            """
            sql = """SELECT * FROM %s
                     WHERE mac = (%s) OR ip = (%s)
                     AND (
                          ((%s) <= start AND start <= (%s)) OR
                          ((%s) <= stop AND stop <= (%s)) OR
                          (start <= (%s) <= stop)
                         )
                     ORDER BY stop DESC, id DESC
                     LIMIT 1;"""
            data = (AsIs(table),
                    values[0], values[1],
                    adjusted_start, adjusted_stop,
                    adjusted_start, adjusted_stop,
                    adjusted_start)
        else:
            sql = """SELECT * FROM %s
                     WHERE (%s) = ('%s')
                     AND (
                          ((%s) <= start AND start <= (%s)) OR
                          ((%s) <= stop AND stop <= (%s)) OR
                          (start <= (%s) AND (%s) <= stop)
                         )
                     ORDER BY stop DESC, id DESC
                     LIMIT 1;"""
            data = (AsIs(table),
                    AsIs(fields[0]), AsIs(values[0]),
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
        return results[0] if e.matches(event) else None
