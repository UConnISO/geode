from geode.splunk import Splunk
from geode.database import Database
from geode.event import Event
import geode.utils as utils
import datetime
import logging
import sys

class Geode:
    def __init__(self, log_file='/var/log/geode/justin_test_geode.log'):
        """On instantiation, turn on logging"""
        logging.basicConfig(filename=log_file, level=logging.INFO)
        
    def _connect(self,tableoption):
        while(True):
            print("connecting")
            try:
                self.splunk = Splunk()
                print('splunk connected')
                sys.stdout.flush()
                self.database = Database(tableoption)
                print('database connected')
                sys.stdout.flush()
            except Exception as e:
                print("waiting to connect")
                logging.exception("Unable to connect: {0}".format(e))
                utils.wait()
                continue
            return None
 
    def process_results_DHCP(self, results, s, debugoption):
        DHCP_LATENCY_BUFFER=30 #this is in seconds
        for r in results:
            r = Event(r)
            lookup = self.database.select_dhcp(r,DHCP_LATENCY_BUFFER)
            #sets latency back thirty seconds
            #checks for complete overlap
            #looks only at last result
            if lookup is not None:
                if r.get('ip')==lookup.get('ip'):
                    self.database.update_stop_time(r.get('stop'), lookup.get('id'))
                    #Sets stop time to r's stop time, because it is the most recent dhcp lease
                else:
                    self.database.terminate_DHCP(lookup, r)
            else:
                self.database.insert(r)
            if(debugoption==0):
                tag = 'earliest_%s_time' % s
                earliest_time = r.get('start')
                utils.update_config('Time', tag, earliest_time)
    
    def process_results_INFO(self, results, s, debugoption, table_option):
        keys_for_match=('netid','hostname')
        for r in results:
            r = Event(r)
            if ("info" in table_option) and (debugoption==1):
                     self.database.insert(r)
            else:
                lookup = self.database.select_info(r)
                if lookup is not None:
                    if debugoption==-1:
                        pass#set later, must do this for threading
                    else:   
                        to_do=r.matches_info(lookup,keys_for_match)
                        #returns 0: conflicting info
                        #returns 1: new info, merge and update
                        #returns 2: same info, don't bother merging
  
                        ###NOTE:setting keys to just netid and hostname is not OO, but is much faster
                        ###If you want to add other keys,change the todo==1 to this:
                        #m=lookup.merge(r)
                        #self.database.update(m,lookup.get('id')
                        if to_do==1:                       
                            self.database.update_netid_hostname(r.get('netid'),r.get('hostname'),lookup.get('id'))
                        elif to_do==0:
                            self.database.terminate_info(lookup, r)
            #   else:
            #        self.database.insert(r)#For normal searches this shouldn't run
            # If we do, and an info event overlaps with a dhcp and an info event,
            # it could map to the info event even tho the info event is useless!
            #Additionally, inserting random info into db is not point of geode?
            ###On top of that, after the first round of dhcp requests go through,
            ###then all info should map to a dhcp, and if it doesnt then there is a
            ###problem with dhcp!!! We should never need to do this anyways
            if(debugoption==0):
                tag = 'earliest_%s_time' % s
                earliest_time = r.get('start')
                utils.update_config('Time', tag, earliest_time)


    def process_results(self, results, s, debugoption):
        DEFAULT_BUFFER=30
        """Process results from Splunk, inserting them into the database"""
        for r in results:
            print("DO NOT RUN THIS!!!! THIS IS OLD AND SHOULD NEVER BE RUN!!!")
            r = Event(r)
            lookup = self.database.select_mac_ip(r,DEFAULT_BUFFER)
            if lookup is not None:
                if r.matches_netid_hostname_ip_other(lookup):#since mac, ip already matches, just check if hostname and netid match
                    m = lookup.merge(r)
                    self.database.update(m, lookup.get('id'))
                else:
                    self.database.terminate(lookup, r.get('start'))
                    self.database.insert(r)
            else:
                self.database.insert(r)
            if(debugoption==0):
                tag = 'earliest_%s_time' % s
                earliest_time = r.get('start')
                utils.update_config('Time', tag, earliest_time)

    def process_results_new(self,results,s,debugoption,table_option):
        print("processing")
        sys.stdout.flush()
        if "dhcp" in s:
            self.process_results_DHCP(results,s,debugoption)
        elif "info" in s:
            self.process_results_INFO(results,s,debugoption,table_option)
        else:
            print("search not recognized")
        print("just processed: "+str(s))
        sys.stdout.flush()

    def search(self,debugoption,table_option, latest_time):
        searches = utils.get_search_names(raw=True)
        print('reached search block')
        sys.stdout.flush()
        if debugoption==1:
            utils.list_debug_times(latest_time)
            if ("dhcp" in table_option):
                searches=["dhcp"]
            elif("info" in table_option):
                searches=["info"]            
        for s in searches:
            results = self.splunk.search(s, latest_time,debugoption)
            try:
                self.process_results_new(results, s,debugoption,table_option)
            except Exception as e:
                print(e)
                logging.exception(str(e))
                break
        print('just processed the last search')
    def main(self,debugoption,table_option):
        """Main function that run the searches and processes results"""

        while (True):
            latest_time=utils.set_latest_time(table_option)
            self._connect(table_option)
            if(table_option=="create_tables"):
                self.database._create_debug_tables()
                break
            self.search(debugoption,table_option, latest_time)
            if debugoption!=0:
                break
                

if __name__ == "__main__":
    option=-1
    while(True):
        debugoption=utils.list_starting_options()   
        if(debugoption==0):
            Geode().main(0,"sediment")
        elif(debugoption==1):   
            option=utils.list_debug_options()
            if(option==0):
                utils.change_debug_default_time()            
            elif(option==1):
                Geode().main(1,"create_tables")
            elif(option==2):
                pass
#debug two tables, also dhcp/info unmerged to merge, and dhcp info merged to total, and total unmerged to total merged. looks in table_info to check that times of tables match
            elif(option==3):
                tables_list=utils.prompt_for_table_option()
                for table_option in tables_list:
                    print("populating "+table_option)
                    Geode().main(debugoption,table_option) 
            else:
                break
        elif(debugoption==-1):
            pass#put threading here
        else:
            break
