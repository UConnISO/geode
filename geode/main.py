from geode.splunk import Splunk
from geode.database import Database
import geode.utils as utils


class Geode:

    def __init__(self):
        self.splunk = Splunk()
        self.database = Database()

    def main(self):
        """Main function that run the searches and process results"""

        # For each of the searches, run the search and process the results
        # from that search
        while True:
            searches = utils.get_searches(raw=True)
            for s in searches:
                results = self.splunk.search(s)
                self.process_results(results)

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
                self.database.update(m)
            # Otherwise, the information is conflicting, so terminate the old
            # event and make a new one. The termination happens at the start
            # time of the new event, since this is the first time that we know
            # for certain the old event does not match
            else:
                self.database.terminate(lookup, r.get('start'))
                self.database.insert(r)


if __name__ == "__main__":
    Geode().main()
