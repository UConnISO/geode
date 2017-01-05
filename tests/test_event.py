import unittest
import datetime

# Our class imports
from geode.event import Event


class EventTestCase(unittest.TestCase):
    """Test class for testing various functionalities of Geode"""

    def setUp(self):
        """Create various events that we will use in this file"""

        # this is an event from splunk
        self.event1 = Event(
                      {
                        'ip': '',
                        'mac': 'ff:ff:ff:ff:ff:ff',
                        'netid': 'abc12345',
                        'start': '2016-11-22T11:11:22',
                        'stop': '2016-11-22T11:41:22',
                        'event_type': 'wireless_authentication'
                      })

        # this is an event from postgres
        self.event2 = Event(
                      {
                        'id': 1,
                        'ip': '127.0.0.1',
                        'mac': 'ff:ff:ff:ff:ff:ff',
                        'netid': '',
                        'start': datetime.datetime(2016, 11, 22, 11, 22),
                        'stop': datetime.datetime(2016, 11, 22, 21, 11),
                        'event_type': [1, 2]
                      })

        self.event3 = Event(
                      {
                       'ip': '127.0.0.1',
                       'start': '2016-12-12T00:00:00',
                       'stop': '2016-12-12T00:30:00',
                       'user_agent': 'test user agent string'
                      })

    def test_event_creation(self):
        """ Test to make sure that event creation happens as we expect """

        e = self.event1
        self.assertEquals(e.get('ip'), None)
        self.assertEquals(e.get('mac'), 'ff:ff:ff:ff:ff:ff')
        self.assertEquals(e.get('netid'), 'abc12345')
        self.assertEquals(e.get('start'),
                          datetime.datetime(2016, 11, 22, 11, 11, 22))
        self.assertEquals(e.get('stop'),
                          datetime.datetime(2016, 11, 22, 11, 41, 22))
        self.assertEquals(e.get('event_type'),
                          set(['wireless_authentication']))
        self.assertEquals(e.get('id'), None)

        e2 = self.event2
        self.assertEquals(e2.get('id'), 1)
        self.assertEquals(e2.get('ip'), '127.0.0.1')
        self.assertEquals(e2.get('mac'), 'ff:ff:ff:ff:ff:ff')
        self.assertEquals(e2.get('netid'), None)
        self.assertEquals(e2.get('start'),
                          datetime.datetime(2016, 11, 22, 11, 22, 00))
        self.assertEquals(e2.get('stop'),
                          datetime.datetime(2016, 11, 22, 21, 11, 00))
        self.assertEquals(e2.get('event_type'), set(['DHCPACK', 'DHCPEXPIRE']))

        e3 = self.event3
        self.assertEquals(e3.get('ip'), '127.0.0.1')
        self.assertEquals(e3.get('mac'), None)
        self.assertEquals(e3.get('start'),
                          datetime.datetime(2016, 12, 12, 00, 00, 00))
        self.assertEquals(e3.get('stop'),
                          datetime.datetime(2016, 12, 12, 00, 30, 00))
        self.assertEquals(e3.get('user_agent'), 'test user agent string')
        self.assertEquals(e3.get('os'), None)

    def test_match(self):
        """ Test the match functionality of events """
        e1 = self.event1
        e2 = self.event2
        e3 = self.event3
        self.assertTrue(e1.matches(e2))
        self.assertTrue(e1.matches(e1))
        self.assertTrue(e2.matches(e2))
        self.assertFalse(e1.matches(e3))
        self.assertFalse(e2.matches(e3))

    def test_merge(self):
        """ Test the functionality of merging of events"""
        e1 = self.event1
        e2 = self.event2
        # What the merged event should look like
        tmp = Event(
                    {
                     'id': 1,
                     'ip': '127.0.0.1',
                     'mac': 'ff:ff:ff:ff:ff:ff',
                     'netid': 'abc12345',
                     'start': datetime.datetime(2016, 11, 22, 11, 11, 22),
                     'stop': datetime.datetime(2016, 11, 22, 21, 11, 00),
                     'event_type': [1, 2, 6]
                    }
                   )
        self.assertEquals(tmp, e1.merge(e2))


if __name__ == '__main__':
    unittest.main()
