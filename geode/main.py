from geode.splunk import Splunk
from geode.database import Database
import geode.utils as utils

import logging


class Geode:

    def __init__(self, log_file='/var/log/geode/geode.log'):
        """On instantiation, turn on logging"""
        logging.basicConfig(filename=log_file, level=logging.DEBUG)

    def _connect(self):
        """Connect to Splunk and to the Database"""

        # Connect to the things we need to connect to
        self.splunk = Splunk()
        self.database = Database()

    def process_results(self, results):
        """Process results from Splunk, inserting them into the database"""

        # For each of the results:
        for r in results:
            # First check to see if there is another event that spans this time
            # in the database
            lookup = self.database.select(r)

            # If there is, and the event matches, then merge these events and
            # update the database
            if r.matches(lookup):
                m = lookup.merge(r)
                self.database.update(m, lookup.get('id'))
            # Otherwise, the information is conflicting, so terminate the old
            # event and make a new one. The termination happens at the start
            # time of the new event, since this is the first time that we know
            # for certain the old event does not match
            else:
                self.database.terminate(lookup, r.get('start'))
                self.database.insert(r)

    def main(self):
        """Main function that run the searches and processes results"""

        # For each of the searches, run the search and process the results
        # from that search
        while True:

            try:
                self._connect()
            except Exception as e:
                logging.error("Unable to connect: {0}".format(e))
                utils.wait()
                continue

            searches = utils.get_searches(raw=True)
            for s in searches:
                results = self.splunk.search(s)
                self.process_results(results)


if __name__ == "__main__":
    Geode().main()
