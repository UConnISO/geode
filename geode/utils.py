import datetime


def calc_time_diff_string(d_string, duration):
    '''
    Takes in a datetime string and returns the diffence in seconds (duration)
    Use a negative int to go back in time
    Returns a string of the new datetime
    '''

    if not isinstance(d_string, str):
        print('Invalid argument for calc_time_diff_string: String expected')
        return None
    else:
        new_dt_dto = (datetime.datetime.strptime(d_string, "%Y-%m-%dT%H:%M:%S")
                      + datetime.timedelta(seconds=int(duration)))
        new_dt_string = datetime.datetime.strftime(new_dt_dto,
                                                   "%Y-%m-%dT%H:%M:%S")

        return new_dt_string


def return_difference(earliest_time, latest_time):
    '''
    Takes two time strings and returns the difference between them
    '''

    earliest_dto = string_to_dto(earliest_time)
    latest_dto = string_to_dto(latest_time)
    diff = latest_dto - earliest_dto
    total_seconds = diff.days*24*60*60 + diff.seconds

    return total_seconds


def string_to_dto(dt_string):
    '''
    Kind of trivial, but allows us to do it in less keystrokes
    And have some control of the format in case we need to change it
    Then we can just change it here rather than all over our code

    Takes in a datetime string and returns a date time object
    '''

    if not isinstance(dt_string, str):
        print('Invalid argument exception for string_to_dto: String expected')

    dt_dto = datetime.datetime.strptime(dt_string, "%Y-%m-%dT%H:%M:%S")
    return dt_dto
