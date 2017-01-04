"""This file is for testing the functionality of Splunk"""
from geode.splunk import Splunk


def test_search():
    splunk = Splunk()
    for r in splunk.search("dhcp"):
        earliest_time = r.get('start')


if __name__ == "__main__":
    test_search()
