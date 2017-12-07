import datetime
from ConfigParser import SafeConfigParser as SCP
import time

def prompt_for_table_option():
    tables_to_run=[]
    while(True):
        duration=prompt_for_duration_to_string()
        kind_of_table=prompt_for_table_to_string()
        for table in get_tables(1):
            if (duration in table) and (kind_of_table in table):
                tables_to_run.append(table)
        end=int(input("If you want to end, enter one: "))
        if(end):
            break
    return tables_to_run

def prompt_for_duration_to_string():
    option=-1
    table_duration="not a substring"
    while(option<0 or option>3):
        print("Would you like to run this for one minute, one hour, or one day?")
        print("One min:   0")
        print("One hr:    1")
        print("One day:   2")
        print("Exit:      3")
        try:
            option = int(input("Enter option: "))
            if option==0:
                table_duration="one_min"
            elif option==1:
                table_duration="one_hr"
            elif option==2:
                table_duration="one_day"
        except ValueError:
            print("That's not an int!")
    return table_duration

def prompt_for_table_to_string():
    option=-1
    table="not a substring"
    while(option<0 or option>4):
        print("Would you like to run this for one minute, one hour, or one day?")
        print("New data       0")
        print("Correct Data:  1")
        print("DHCP:          2")
        print("Info:          3")
        print("Exit:          4")
        try:
            option = int(input("Enter option: "))
            if option==0:
                table="data_to_check"
            elif option==1:
                table="correct_data"
            elif option==2:
                table="dhcp"
            elif option==3:
                table="info"
        except ValueError:
            print("That's not an int!")
    return table


def prompt_for_search(searches):
    option=-1
    while(option<0 or option>len(searches)-1):
        print("Which search?")
        for i in range(0,len(searches)):
            print(searches[i]+": "+i)
        try:
            option = int(input("Enter option: "))
        except ValueError:
            print("That's not an int!")
    return option


def list_debug_times(latest_time):
    print("running a search, using earliest time for dhcp as:")
    print(read_config('Time','test_earliest_dhcp_time'))
    print('and for info as')
    print(read_config('Time','test_earliest_info_time'))
    print("And the latest time as:")
    print(dto_to_string(latest_time))

def set_latest_time(table_option):
    latest_time=time_diff(now(), -300)
    if("min" in table_option):
        latest_time=reset_debug_times(60)#debug for one min
    elif("hr" in table_option):
        latest_time=reset_debug_times(3600)#debug for one hr
    elif("day" in table_option):
        latest_time=reset_debug_times(86400)#this is in seconds for one day
    return latest_time

def get_tables(option):
    #NOTE: CHANGE THIS TABLE IN ANY WAY AND IT WILL AFFECT THE WHOLE PROGRAM
    __tables=("sediment","data_to_check_one_min","data_to_check_one_hr","data_to_check_one_day","correct_data_one_min","correct_data_one_hr","correct_data_one_day","one_min_dhcp","one_hr_dhcp","one_day_dhcp","one_min_info","one_hr_info","one_day_info")
    if option==0:
        return __tables
    else:
        __debug_tables=[]
        for i in xrange(0,len(__tables)):
            if __tables[i]!="sediment" and __tables[i]!="table_info": 
                __debug_tables.append(__tables[i])
        return tuple(__debug_tables)
def convert_table_option(table_option):
    temp=get_tables(1)
    if table_option in range(0,len(temp)):
        table_option=temp[table_option]
        print(table_option)
    else:
        print("no!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    return table_option

def prompt_for_duration():
    option=-1
    while(option<0 or option>4):
        print("Would you like to run this for one minute, one hour, one day, or all?")
        print("One min:   0")
        print("One hr:    1")
        print("One day:   2")
        print("All times: 3")
        print("Exit:      4")
        try:
            option = int(input("Enter option: "))
        except ValueError:
            print("That's not an int!")
    return option


def list_starting_options():
    option=-1
    while(option<0 or option>2):
        print("Options:")
        print("0 to run, 1 to list debugging options, 2 to exit")
        try:
            option = int(input("Enter option: "))          
        except ValueError:
            print("That's not an int!")
    return option
        
def list_debug_options():
    option=-1
    while(option<0 or option>4):
        print("Options:")
        print("Change default debug time:                    0")
        print("Create tables that have not yet been created: 1")
        print("Debug two tables:                             2")
        print("Populate Tables:                              3")
        print("Exit:                                         4")
        try:
            option=int(input("Enter option: "))
        except ValueError:
            print("That's not an int!")
    return option
            

    
def now():
    """Get the current time (in UTC to avoid daylight savings issues)"""
    return datetime.datetime.utcnow()


def time_diff(dto, duration):
    """Returns a new datetime object that is "duration" seconds different"""
    return dto + datetime.timedelta(seconds=int(duration))


def time_diff_string(d_string, duration):
    """Takes in a datetime string and returns the difference in seconds
    Use a negative int to go back in time
    Returns a string of the new datetime
    """

    new_dt_dto = (datetime.datetime.strptime(d_string, "%Y-%m-%dT%H:%M:%S")
                  + datetime.timedelta(seconds=int(duration)))
    new_dt_string = datetime.datetime.strftime(new_dt_dto,
                                               "%Y-%m-%dT%H:%M:%S")

    return new_dt_string


def return_difference(earliest_time, latest_time):
    """Takes two time strings and returns the difference between them"""

    earliest_dto = string_to_dto(earliest_time)
    latest_dto = string_to_dto(latest_time)
    diff = latest_dto - earliest_dto
    total_seconds = diff.days*24*60*60 + diff.seconds

    return total_seconds


def string_to_dto(dt_string):
    """Takes in a datetime string and returns a date time object
    Kind of trivial, but allows us to do it in less keystrokes
    And have some control of the format in case we need to change it
    Then we can just change it here rather than all over our code

    """

    dt_dto = datetime.datetime.strptime(dt_string, "%Y-%m-%dT%H:%M:%S")
    return dt_dto


def dto_to_string(dto):
    """Converts a datetime object to a string"""
    return dto.strftime("%Y-%m-%dT%H:%M:%S")


def read_config(section, tag, raw=False, path="/etc/geode/justin_test_settings.conf"):
    """Reads the specified section from the configuration file"""
    parser = SCP()
    parser.read(path)

    return parser.get(section, tag, raw=raw)


def update_config(section, tag, text, path="/etc/geode/justin_test_settings.conf"):
    """Updates the configuration file with the provided information"""
    if text is None:
        text = ""
    if type(text) is datetime.datetime:
        text = dto_to_string(text)
    parser = SCP()
    parser.read(path)
    parser.set(section, tag, text)
    with open(path, 'wb') as c:
        parser.write(c)


def get_search_names(path="/etc/geode/justin_test_settings.conf", raw=False):
    """Gets all of the search in the config file"""
    parser = SCP()
    parser.read(path)

    return [search[0] for search in parser.items("Searches", raw=raw)]

def reset_debug_times(duration):
    test_time=read_config('Time','test_earliest_time')
    update_config('Time', 'test_earliest_info_time', test_time)
    update_config('Time', 'test_earliest_dhcp_time', test_time)
    latest_time=time_diff_string(test_time, duration)#note that duration is in seconds
    return string_to_dto(latest_time)

def change_debug_default_time():
    option=-1
    while(option<0 or option>1):
        print("Are you sure?")
        print("This will invalidate all debug data")
        print("Only do this if the data no longer exists")
        print("0 to continue, 1 to exit: ")
        try:
            option=int(input("Enter option: "))
        except ValueError:
            print("That's not an int!")
    if option==1:
        return
    year=input("Enter year ex:2017 : ")
    month=input("Enter month ex: 10 :")
    day=input("Enter day of the month ex: 30 :")
    hour=input("Enter Hour ex: 11 :")
    minute=input("Enter minute ex: 37 :")
    second=input("Enter second ex: 43 :")
    test_time=datetime.datetime(year, month, day, hour, minute, second)
    update_config('Time', 'test_earliest_time', test_time)
    reset_debug_times(60)

def wait(amount=60):
    """The amount of time to wait before trying to reconnect to services"""
    time.sleep(amount)
