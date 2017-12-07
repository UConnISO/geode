import datetime
import copy
import geode.utils as utils

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
             'access_combined': 9
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
                     9: 'access_combined'
                    }

    def __init__(self, d):
        """Constructor; takes in a dict and copies the values to this object"""

        # Make sure that we're given something that we can work with
        if not isinstance(d, dict):
            raise Exception('Invalid type: %s is not dict' % str(type(d)))

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

    def matches(self, e, DEFAULT_BUFFER):
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
                if not self._does_overlap(e,DEFAULT_BUFFER):
                    print("this statement should never execute! I'm in event.match!!!!(it shouldnt because it should already overlap to even be here))")
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
    def matches_netid_hostname_ip_new(self, e):
        """Returns True if this event and e have no conflicting information"""
        ####OUTDATED AND ALSO BROKEN
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
        if 'start' in keys:
            keys.remove('start')
        if 'mac' in keys:
            keys.remove('mac')

        
        # For all of the keys that we do care about, check to make sure that
        # there is no conflicting evidence between the two events
        #######under this new match, if one event has netid and the other doesn't, they do not match!
        for key in keys:
            if((e.get(key) is not (None or '')) or (self.get(key) is not (None or ''))):
                if((e.get(key) is not (None or '') and self.get(key) is (None or '')) or (e.get(key) is (None or '') and self.get(key) is not (None or '')) or (self.get(key) != e.get(key))):
                # There is conflicting evidence, so return
                    return False
        # If we got here, there is no conflicting evidence
        print("we are about to merge an event:::::::::::::::")
        print("For db result: mac: "+str(e.get('mac'))+"ip: "+str(e.get('ip'))+"netid: "+str(e.get('netid')))
        print("For new result: mac: "+str(self.get('mac'))+"ip: "+str(self.get('ip'))+"netid: "+str(self.get('netid')))
        if(e.get('netid')!=self.get('netid')):
            print('notequal')
            print(keys)
        return True
        



    def matches_dhcp(self, e):
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
        if 'start' in keys:
            keys.remove('start')
        if 'mac' in keys:
            keys.remove('mac')


        # For all of the keys that we do care about, check to make sure that
        # there is no conflicting evidence between the two events
        for key in keys:
            # Check to make sure that the start and stop times overlap
            # The below commented out statement is equivalent to what we are
            # actually running (because of DeMorgan's Law), but I find what is
            # commented out easier to read, so that's why it's there
            #
            # if e.get(key) is None or self.get(key) == e.get(key):
            if (e.get(key) is not None and
                  e.get(key) is not '' and
                  self.get(key) is not None and
                  self.get(key) is not '' and
                  self.get(key) != e.get(key)):
                # There is conflicting evidence, so return
                  return False

        # If we got here, there is no conflicting evidence
        return True
    def matches_info(self, lookup,keys):
        return_value=2#by default, everything matches, and self has no new info for lookup
        for key in keys:
            if (self.get(key) is not None and self.get(key)!=lookup.get(key)):
                if lookup.get(key) is not None:
                    return 0#conflicting info
                else:
                    return_value=1#new info, but still matches 
        return return_value
    
    def merge(self, e):
        """Merges two events together
        This means taking all of the values from e and all of the values from
        self and putting them into one event


        """
        #default stop to start 
        if not self.get('stop'):
            self['stop'] = (self.get('start'))
        if not e.get('stop'):
            e['stop'] = (e.get('start'))

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
            tmp[k] = e.get(k)
        return tmp

    def _does_overlap(self, e,DEFAULT_BUFFER):
        """Returns True if time times of this event and e overlap
        The four cases for overlap are as follows:

             |----self---|
        1)     |--e2--|
        2) |----e2----|
        3)         |----e2----|
        4) |--------e2--------|

        """
        print("this function is alread run in sql queries so should never be used, and also it is not ok to add a buffer, this screws up and can be proven in test cases")
        #added the default buffer to allow better overlap checking
        if type(e.get('start')) is str:
            adjusted_start = utils.time_diff_string(e.get('start'),-DEFAULT_BUFFER)
        else:
            adjusted_start = utils.time_diff(e.get('start'),-DEFAULT_BUFFER)
        if type(e.get('stop')) is str:
            adjusted_stop = utils.time_diff_string(e.get('stop'), DEFAULT_BUFFER)
        else:
            adjusted_stop = utils.time_diff(e.get('stop'), DEFAULT_BUFFER)

        self_start=self.get('start')
        self_stop=self.get('stop')
        if((self_start<=adjusted_start<=self_stop)
            or
            (self_start<=adjusted_stop<=self_stop)
            or
            (adjusted_start<=self_start and self_stop<=adjusted_stop)):
            return True
        return False
#Used to be code below, which didn't have adjusted start and stop times, and also had extra lines that where unneccesary
#        if ((self.get('start') <= e.get('start') <= self.get('stop') and
#                self.get('stop') >= e.get('stop') >= self.get('start'))
#            or
#            (self.get('start') >= e.get('start') and
#                #self.get('stop') >= e.get('stop') >= e.get('start'))
#                self.get('stop') >= e.get('stop') >= event.get('start')
#            or
#            (self.get('start') <= e.get('start') <= self.get('stop') and
#                self.get('stop') <= e.get('stop'))
#            or
#            (self.get('start') >= e.get('start') and
#                self.get('stop') <= e.get('stop'))):
#
#            return True
#        return False
