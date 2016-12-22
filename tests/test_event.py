import unittest
import datetime

# Our class imports
from geode.event import Event


class EventTestCase(unittest.TestCase):
    """Test class for testing various functionalities of Geode"""

    def setUp(self):
        """Create various events that we will use in this file"""

        # this is an event from splunk
        self.event1 = {
                        'ip': '',
                        'mac': 'ff:ff:ff:ff:ff:ff',
                        'netid': 'djm13029',
                        'start': '2016-11-22T11:11:22',
                        'stop': '2016-11-22T11:41:22',
                        'event_type': 'wireless_authentication'
                      }

        # this is an event from postgres
        self.event2 = {
                        'id': 1,
                        'ip': '127.0.0.1',
                        'mac': 'ff:ff:ff:ff:ff:ff',
                        'netid': '',
                        'start': datetime.datetime(2016, 11, 22, 11, 22),
                        'stop': datetime.datetime(2016, 11, 22, 21, 11),
                        'event_type': [1, 2]
                      }

    def test_event_creation(self):
        """ Test to make sure that event creation happens as we expect """

        e = Event(self.event1)
        self.assertEquals(e.get('ip'), None)
        self.assertEquals(e.get('mac'), 'ff:ff:ff:ff:ff:ff')
        self.assertEquals(e.get('netid'), 'djm13029')
        self.assertEquals(e.get('start'),
                          datetime.datetime(2016, 11, 22, 11, 11, 22))
        self.assertEquals(e.get('stop'),
                          datetime.datetime(2016, 11, 22, 11, 41, 22))
        self.assertEquals(e.get('event_type'),
                          set(['wireless_authentication']))
        self.assertEquals(e.get('id'), None)

        e2 = Event(self.event2)
        self.assertEquals(e2.get('id'), 1)
        self.assertEquals(e2.get('ip'), '127.0.0.1')
        self.assertEquals(e2.get('mac'), 'ff:ff:ff:ff:ff:ff')
        self.assertEquals(e2.get('netid'), None)
        self.assertEquals(e2.get('start'),
                          datetime.datetime(2016, 11, 22, 11, 22))
        self.assertEquals(e2.get('stop'),
                          datetime.datetime(2016, 11, 22, 21, 11))
        self.assertEquals(e2.get('event_type'), set(['DHCPACK', 'DHCPEXPIRE']))

    def test_match(self):
        """ Test the match functionality of events """
        self.assertTrue(self.event1.matches(self.event2))

    def test_merge(self):
        pass

if __name__ == '__main__':
    unittest.main()
