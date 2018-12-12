"""This file is for testing two searches and comparing performance of each search"""

import splunk
from multiprocessing import Pool
import time
from configparser import SafeConfigParser

def search_splunk(search):
    c = SafeConfigParser()
    c.read("/etc/geode/settings.conf")
    s = splunk.Splunk(**{"username": c.get("Splunk", "username"),
                           "password": c.get("Splunk", "password"),
                           "host":     c.get("Splunk", "host"),
                           "port":     c.get("Splunk", "port")})
    begin = time.time()
    start = "2018-01-16T13:36:16"
    stop = "2018-01-16T13:51:16"
    
    results = s.search(search, start, stop)
    for r in results:
        pass
    end = time.time()
    return round(end-begin, 2)

def main():
    search1 = '''
    search sourcetype=access_combined index=main clientip=137.99.0.0/16 OR clientip=50.28.128.0/18 OR clientip=67.221.64.0/19 OR clientip=10.0.0.0/8 AND netid=* AND (host=rhap* OR host=cas*) |
        rename clientip as ip |
        dedup _time ip netid |
        fields _time netid ip useragent sourcetype |
        eval start=strftime(_time, "%Y-%m-%dT%H:%M:%S") |
        eval event_type=sourcetype |
        table start ip netid useragent event_type|
        sort +start
    '''
    search2 = '''
    search sourcetype=access_combined index=main useragent=* clientip=137.99.0.0/16 OR clientip=50.28.128.0/18 OR clientip=67.221.64.0/19 OR clientip=10.0.0.0/8 AND NOT (useragent="mod_auth_cas*" OR useragent="-") |
        rename clientip as ip |
        rename user as netid |
        eval netid= if(netid=="-",'',netid) |
        dedup _time ip useragent |
        fields _time netid ip useragent sourcetype |
        eval start=strftime(_time, "%Y-%m-%dT%H:%M:%S") |
        eval event_type=sourcetype |
        table start ip netid useragent event_type|
        sort +start
    '''
    search3 = ''' search
	sourcetype=dhcp DHCPACK OR DHCPEXPIRE OR DHCPRELEASE OR RELEASE |
	rename client_ip as ip |
	rex field=_raw "(?<mac>([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2}))" |
	rex field=_raw "to ([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2}) \((?<hostname>\S+)\)" |
	rex field=_raw "(?<event_type>DHCPACK|DHCPEXPIRE|DHCPRELEASE|RELEASE)" |
	eval time=strftime(_time, "%Y-%m-%dT%H:%M:%S") |
	dedup mac time event_type |
	rename time as start |
	fillnull value=1200 lease_time |
	eval stop=strftime(strptime(start,"%Y-%m-%dT%H:%M:%S") + lease_time,"%Y-%m-%dT%H:%M:%S") |
	table start stop mac ip hostname event_type |
	sort +start
    '''
    searches = [search1, search2]

    pool = Pool(processes=2)
    results = pool.map(search_splunk, searches)
    pool.close()
    pool.join()

    for r in results:
        print ("%.2f" % r)


if __name__ == "__main__":
    main()



