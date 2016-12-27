import splunklib.client as client
import splunklib.results as results
import io
import sys
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

    def __init__(self, config_file='/etc/geode/test_settings.conf',
                 max_events=10000, search_time=-300,
                 log_file='/var/log/geode/geode.log'):

        self.config_file = config_file
        self.max_events = max_events
        self.search_time = search_time
        self._connect()
        logging.basicConfig(filename=log_file, level=logging.DEBUG)

    def _connect(self):
        """
        Connects to your Splunk instance based on the credentials
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
            logging.error('Splunk connection failure: {0}'.format(str(e)))
            print('Splunk connection failure: {0}'.format(str(e)))

    def search(self, search, latest_time):
        """
        Searches Splunk and sets a result stream to read the events returned

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

        done = False
        caught_up = False
        events_done = False

        search_string = utils.read_config('Searches', search, raw=True)

        # Try to read the time for the particular search from the configuration
        # file. It is possible that this will fail because it won't be there
        # yet, so if that is the case, then set it to be 5 minutes ago
        try:
            tag = 'earliest_%s_time' % search
            earliest_time = utils.read_config('Time', tag)
        except:
            earliest_time = utils.calc_time_diff_string(latest_time, -300)

        while not done:
            """reset events_done to be False every time through the loop.
            This is because if we are not caught up, we are searching 5 minute
            intervals until we get caught up. One of these 5 minute intervals
            could have fewer than the max number of events which means that
            events_done would be set to True.
            Thus, we would never search the next 5 minute interval
            """

            events_done = False

            # Case: We are lagging behind or catching up
            #       therefore, earliest_time is more than 5 mins behind now()
            #       set latest time to earliest+5min then loop back through
            if ((earliest_time is not None) and
                (latest_time is not None) and
                (utils.return_difference(earliest_time, latest_time) > 300)):

                latest_time_temp = utils.calc_time_diff_string(earliest_time,
                                                               300)
                kwargs_search = {"search_mode": "normal",
                                 "earliest_time": earliest_time,
                                 "latest_time": latest_time_temp}

            # Case: We are caught up and on schedule, earliest and latest
            #       don't need any changes
            elif ((earliest_time is not None) and
                  (latest_time is not None) and
                  (utils.return_difference(earliest_time, latest_time)
                   <= 300)):

                kwargs_search = {"search_mode": "normal",
                                 "earliest_time": earliest_time,
                                 "latest_time": latest_time}
                caught_up = True

            # Case: Times are not set in the configuration, so we need to set
            # them. This is probably the first time running this script
            elif ((earliest_time is None) and
                  (latest_time is None)):

                kwargs_search = {"search_mode": "normal", "count": 0}
                caught_up = True

            # Case: Something went horribly wrong, how did we get here?
            else:
                logging.error("Splunk ran into a time comparison issue")
                sys.exit(1)

            while not events_done:
                jobs = self.connection.jobs
                job = jobs.create(search_string, **kwargs_search)
                result_count = job["resultCount"]
                rs = job.results(count=0)

                # Result generator to geode module
                for result in results.ResultsReader(io.BufferedReader(
                                      ResponseReaderWrapper(rs))):
                    earliest_time = result.get('start')
                    yield result

                job.cancel()

                # Check to see if we returned max results
                # We can paginate if needed
                if int(result_count) < int(self.max_events):
                    events_done = True
                else:
                    kwargs_search['earliest_time'] = earliest_time

            utils.update_config('Time', 'earliest_%s_time' %
                                search, earliest_time)

            if caught_up and events_done:
                done = True
