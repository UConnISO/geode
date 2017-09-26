import datetime
from ConfigParser import SafeConfigParser as SCP
import time


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


def read_config(section, tag, raw=False, path="/etc/geode/settings.conf"):
    """Reads the specified section from the configuration file"""
    parser = SCP()
    parser.read(path)

    return parser.get(section, tag, raw=raw)


def update_config(section, tag, text, path="/etc/geode/settings.conf"):
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


def get_search_names(path="/etc/geode/settings.conf", raw=False):
    """Gets all of the search in the config file"""
    parser = SCP()
    parser.read(path)

    return [search[0] for search in parser.items("Searches", raw=raw)]


def wait(amount=60):
    """The amount of time to wait before trying to reconnect to services"""
    time.sleep(amount)
