import os
from datetime import datetime, timedelta
from itertools import islice

import pytz
from etlexceptions import ETLException
from pytz import timezone


def oscmd_rmfile(filename):
    """
    Removes the file by full path
    """
    os.remove(filename)
    return


def gp_validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        return date_str
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(date_str)
        raise ETLException(msg)
    return


def get_runtime_date():
    dtobj = datetime.now(timezone("UTC"))
    return dtobj.strftime("%Y-%m-%d")


def get_timestamp():
    dtobj = datetime.now(timezone("UTC"))
    return dtobj.strftime("%Y-%m-%d %H:%M:%S")


def generate_time_series_from_day(dstr):
    dobj = datetime.strptime(dstr, "%Y-%m-%d")
    tseries = []
    for hour in range(0, 24):
        tseries.append((dobj + timedelta(hours=hour)).strftime("%Y-%m-%d-%H"))
    return tseries


def make_batches_from_pool(pool, batch_size=5):
    for pool_id in range(0, len(pool), batch_size):
        yield iter(pool[pool_id : pool_id + batch_size])


def convert_to_d_h_m_s(total_seconds):
    seconds = int(total_seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    return days, hours, minutes, seconds


def seconds_to_string(run_time):
    return "{0[0]} days, {0[1]} hours, {0[2]} minutes, {0[3]} " "seconds".format(
        convert_to_d_h_m_s(run_time)
    )


def replace_timezone(
    timestring, timezone_to_replace, timestamp_fmt="%a, %d %b %Y %H:%M:%S %z"
):

    dtobj = datetime.strptime(timestring, timestamp_fmt)
    dtobj.replace(tzinfo=pytz.timezone(timezone_to_replace))
    return dtobj


def convert_timezone(timestring, to_timezone, timestamp_fmt="%a, %d %b %Y %H:%M:%S %z"):
    dtobj = datetime.strptime(timestring, timestamp_fmt)
    return dtobj.astimezone(pytz.timezone(to_timezone))


def chunked(iterable, n):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, n))
        if chunk:
            yield chunk
        else:
            return


def chunk_maker(data, chunk_size=5):
    itr = iter(data)
    number_of_chunks = (len(data) // chunk_size) + 1
    total = len(data)
    for x in range(0, number_of_chunks):
        number_of_elements = min(total, chunk_size)
        try:
            chunk = iter([next(itr) for x in range(number_of_elements)])
            total -= number_of_elements
            yield chunk
        except StopIteration:
            print("exception")
