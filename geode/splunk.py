import splunklib.client as client
import splunklib.results as results
import io
import logging

import geode.utils as utils


class ResponseReaderWrapper(io.RawIOBase):
    """Splunk ResultReader wrapper to speed up IO from Splunk
       Credit to senior design team for this solution:
            David Engel
            Kyle Heitman
            Gavin Li
            Tony Pham
            Nathan Ramdial
    """

    def __init__(self, responseReader):
        self.responseReader = responseReader

    def readable(self):
        return True

    def close(self):
        self.responseReader.close()

    def read(self, n):
        return self.responseReader.read(n)

    def readinto(self, b):
        size = len(b)
        data = self.responseReader.read(size)
        for idx, ch in enumerate(data):
            b[idx] = ch

        return len(data)


class Splunk:
    """Contains all the functionality to connect to your Splunk instance,
       search through Splunk, and return results
    """

    def __init__(self, config_file='/etc/geode/settings.conf',
                 max_events=10000, search_time=-300,
                 log_file='/var/log/geode/geode.log'):

        self.config_file = config_file
        self.max_events = max_events
        self.search_time = search_time
        logging.basicConfig(filename=log_file, level=logging.INFO)
        self._connect()

    def _connect(self):
        """Connects to your Splunk instance based on the credentials
        supplied to the class in the configuration file
        """

        # Read in the Splunk settings
        section = "Splunk"

        username = utils.read_config(section, "username")
        password = utils.read_config(section, "password")
        port = utils.read_config(section, "port")
        host = utils.read_config(section, "host")

        # Try to connect else error handle
        try:
            self.connection = client.connect(username=username,
                                             password=password,
                                             port=port,
                                             host=host)

        except Exception as e:
            logging.exception('Splunk connection failure: {0}'.format(str(e)))
            raise e

    def search(self, search, latest_time=utils.time_diff(utils.now(), -300)):
        """Searches Splunk and sets a result stream to read the events returned

        The latest_time is defaulted to be 5 minutes ago (UTC time) because
        Splunk can take some time to index things properly

        The default functionality is searching from last_event_time_seen until
        now(). However, we took a few things into consideration:
        1) If you run the script longer than 5 mins back or if it lags behind
            you need to catch up and search in 5 minute intervals with latest
            time as earliest_time + 5mins rather than now() so it can backfill
            easier.
        2) Our Splunk instance can only return 10,000 results at a time, yours
            may vary. Set this when initializing the class with max_events=#
            If we receive 10,000 events, we need to loop back through from
            last_event_time_seen until the latest original search time as
            documented in the official Splunk Python SDK documentation.
            See: http://dev.splunk.com/view/python-sdk/SP-CAAAER5#paginating

        """

        # Convert the latest time to a string
        latest_time = utils.dto_to_string(latest_time)
        tag = 'earliest_%s_time' % search

        # Get the most recent time that we've searched, or default to -5m
        try:
            earliest_time = utils.read_config('Time', tag)
        except:
            earliest_time = utils.time_diff_string(latest_time, -300)

        # Get the search string
        search_string = utils.read_config('Searches', search, raw=True)

        # caught_up represents if the latest time we have searched is the
        # latest time that we wanted to search
        caught_up = False
        # events_done represents if we are done looping through the events
        # of a particular search
        events_done = False

        # Run the search, loop through, and search again if needed
        while not caught_up:
            # We reset this each time through the loop because we are now
            # running a search again for a new 5 minute interval.
            events_done = False

            # If we need to search more than 5 minutes at a time, then only
            # search for 5 minutes (because otherwise we'll probably return too
            # many results). Otherwise, search the full time period
            if (utils.return_difference(earliest_time, latest_time) > 300):
                search_time = utils.time_diff_string(earliest_time, 300)
            else:
                search_time = latest_time
                caught_up = True

            # The search parameters
            kwargs_search = {"exec_mode": "blocking",
                             "earliest_time": earliest_time,
                             "latest_time": search_time}
            # Now we need to run the search until we are caught up to when we
            # wanted to search until
            while not events_done:
                # Create a job and run the search
                jobs = self.connection.jobs
                job = jobs.create(search_string, **kwargs_search)
                # Get the results and the result count
                result_count = int(job["resultCount"])
                print result_count
                rs = job.results(count=0)
                # Iterate through all of the results using the modified reader
                for result in results.ResultsReader(io.BufferedReader(
                                      ResponseReaderWrapper(rs))):
                    # Update the earliest time to be the most recent time
                    earliest_time = result.get('start')
                    yield result

                # I'm finished with this guy!
                job.cancel()

                # If we returned less than the max number of results, we're
                # done with this iteration of the search
                if (result_count < self.max_events):
                    events_done = True
                # Otherwise, we want to start the search again
                else:
                    kwargs_search['earliest_time'] = earliest_time
