import calendar
from datetime import datetime


def datetime_to_ms(date):
    return int(calendar.timegm(date.timetuple()) * 1000 + date.microsecond / 1000)


def datetime_to_timestamp(date):
    return int(calendar.timegm(date.timetuple()) + date.microsecond / 1000)


def str_to_datetime(dt_str: str) -> datetime:
    """
    dt: "2023-01-01" or "2023/01/01"
    """
    return datetime.strptime(dt_str.replace("-", "/"), '%Y/%m/%d')
