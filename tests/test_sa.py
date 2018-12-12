"""This file is for testing two searches and comparing performance of each search"""

import splunk
from multiprocessing import Pool
import time
from configparser import SafeConfigParser
import geode.utils as utils

def search_splunk(search, earliest_time, latest_time):
    c = SafeConfigParser()
    c.read("/etc/geode/settings.conf")
    s = splunk.Splunk(**{"username": c.get("Splunk", "username"),
                           "password": c.get("Splunk", "password"),
                           "host":     c.get("Splunk", "host"),
                           "port":     c.get("Splunk", "port")})
    results = s.search(search, earliest_time, latest_time)
    for r in results:
        print r

def main():
    searches = utils.get_search_names(raw=True)
    latest_time = utils.time_diff(utils.now(), -300)
    c = SafeConfigParser()
    c.read("/etc/geode/settings.conf")
    for s in searches:
        if s == "sa":
           search_splunk(c.get("Searches", s, raw=True), c.get("Time", str("earliest_%s_time" % s)), latest_time) 
    

if __name__ == "__main__":
    main()



