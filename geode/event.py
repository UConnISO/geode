import datetime
import copy

# The default duration for an event (in seconds)
DEFAULT_DURATION = 30


class Event(dict):
    """This class is used to deal with events from Splunk and Postgres
    It provides an easy way to do comparison, combining, etc."""

    # This dictionary is used for doing lookups between strings and ints
    # to convert Splunk event types into ints stored in the database
    types = {
             'DHCPACK': 1,
             'DHCPEXPIRE': 2,
             'DHCPRELEASE': 3,
             'RELEASE': 4,
             'tomcat': 5,
             'wireless_authentication': 6,
             'saappsrv': 7,
             'WinEventLog:Security': 8,
             'access_combined': 9,
             'cas:prod': 10,
             'NetApp:Audit': 11
            }

    # This dictionary is used for doing lookups between ints and strings
    # to convert database event types into strings
    types_reverse = {
                     1: 'DHCPACK',
                     2: 'DHCPEXPIRE',
                     3: 'DHCPRELEASE',
                     4: 'RELEASE',
                     5: 'tomcat',
                     6: 'wireless_authentication',
                     7: 'saappsrv',
                     8: 'WinEventLog:Security',
                     9: 'access_combined',
                     10: 'cas:prod',
                     11: 'NetApp:Audit'
                    }

    def __init__(self, d):
        """Constructor; takes in a dict and copies the values to this object"""

        # Make sure that we have a start time for every event
        if d.get('start') is None:
            raise Exception("No start time")

        # Copy the values for d into this event
        for k in d.keys():
            # Convert the start and stop times to be datetime objects if they
            # are strings
            if (k == 'start' or k == 'stop') and (type(d[k]) == str):
                self[k] = datetime.datetime.strptime(d[k], '%Y-%m-%dT%H:%M:%S')

            # Convert all event types to be set of strings
            elif k == 'event_type':
                # TODO: Lots of type checking in python?? There's probably
                # a better way to do this

                # If there are multiple event types, do the lookup and convert
                # to strings
                if type(d[k]) == list:
                    self[k] = set([Event.types_reverse.get(x)
                                  if type(x) == int else x for x in d[k]])
                # If there is only one event type as an in, do the lookup
                # and convert to a string
                elif type(d[k]) == int:
                    self[k] = set([Event.types_reverse.get(d[k])])
                # Otherwise, there is only one type and it's a string, so just
                # add it
                else:
                    self[k] = set([d[k]])

            # Because None is easier to work with than both '' and None
            # NOTE: I suppose that this could cause problems, and if it does
            # cause problems for someone, I'm sorry. Suggest the change and
            # I'll alter the code
            elif d[k] == '':
                pass

            else:
                # Use deep copy to prevent annoying bugs that might arise if
                # the values are dictionaries themselves
                self[k] = copy.deepcopy(d[k])

        # Make sure that we have a stop time for every event
        if self.get('stop') is None:
            self['stop'] = self.get('start') + datetime.timedelta(seconds=30)

    def matches(self, e):
        """Returns True if this event and e have no conflicting information"""

        # Get all of the keys for this event
        keys = self.keys()

        # Remove the keys that we don't care about
        if 'id' in keys:
            keys.remove('id')
        if 'event_type' in keys:
            keys.remove('event_type')
        if 'useragent' in keys:
            keys.remove('useragent')
        if 'os' in keys:
            keys.remove('os')
        # Remove stop, since we only really need to check start
        if 'stop' in keys:
            keys.remove('stop')

        # For all of the keys that we do care about, check to make sure that
        # there is no conflicting evidence between the two events
        for key in keys:
            # Check to make sure that the start and stop times overlap
            if key == 'start':
                if not self._does_overlap(e):
                    return False

            # The below commented out statement is equivalent to what we are
            # actually running (because of DeMorgan's Law), but I find what is
            # commented out easier to read, so that's why it's there
            #
            # if e.get(key) is None or self.get(key) == e.get(key):
            elif (e.get(key) is not None and
                  e.get(key) is not '' and
                  self.get(key) is not None and
                  self.get(key) is not '' and
                  self.get(key) != e.get(key)):
                # There is conflicting evidence, so return
                return False

        # If we got here, there is no conflicting evidence
        return True

    def merge(self, e):
        """Merges two events together
        This means taking all of the values from e and all of the values from
        self and putting them into one event

        Neither e nor self are modified during this process
        """

        # if there is no stop time for either event, default it to 30 seconds
        if not self.get('stop'):
            self['stop'] = (self.get('start') +
                            datetime.timedelta(seconds=DEFAULT_DURATION))
        if not e.get('stop'):
            e['stop'] = (e.get('start') +
                         datetime.timedelta(seconds=DEFAULT_DURATION))

        # the merged event; default to have the values of this event
        tmp = copy.deepcopy(self)

        # take the earliest start time
        tmp['start'] = min(self.get('start'), e.get('start'))

        # it's safe to assume that there are no DHCP RELEASE events, so we want
        # to take the latest stop time possible, since if we terminate early,
        # it won't matter which time we picked anyways
        tmp['stop'] = max(self.get('stop'), e.get('stop'))

        keys = e.keys()

        if 'start' in keys:
            keys.remove('start')
        if 'stop' in keys:
            keys.remove('stop')
        if 'event_type' in keys:
            keys.remove('event_type')

        # Add all of the new event types using sets
        tmp['event_type'] |= e.get('event_type')

        # copy e into the new event
        for k in keys:
            tmp[k] = e[k]

        # return the merged event
        return tmp

    def _does_overlap(self, e):
        """Returns True if time times of this event and e overlap
        The four cases for overlap are as follows:

             |----self---|
        1)     |--e2--|
        2) |----e2----|
        3)         |----e2----|
        4) |--------e2--------|

        """

        if ((self.get('start') <= e.get('start') <= self.get('stop') and
                self.get('stop') >= e.get('stop') >= self.get('start'))
            or
            (self.get('start') >= e.get('start') and
                self.get('stop') >= e.get('stop') >= e.get('start'))
            or
            (self.get('start') <= e.get('start') <= self.get('stop') and
                self.get('stop') <= e.get('stop'))
            or
            (self.get('start') >= e.get('start') and
                self.get('stop') <= e.get('stop'))):

            return True
        return False
