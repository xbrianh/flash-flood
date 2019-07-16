import datetime
from collections import namedtuple

def datetime_to_timestamp(dt):
    return dt.strftime("%Y-%m-%dT%H%M%S.%fZ")

def datetime_from_timestamp(ts):
    return datetime.datetime.strptime(ts, "%Y-%m-%dT%H%M%S.%fZ")

class DateRange:
    def __init__(self, start: datetime.datetime, end: datetime.datetime):
        self.start = start
        self.end = end

    def overlaps(self, other):
        """
        Test if [self.start : self.end] overlaps with (other.start : other.end]
        """
        a = self
        b = other
        if b.start <= a.start and a.end <= b.end:
            return True
        elif a.start <= b.start and b.end <= a.end:
            return True
        elif a.start <= b.start and b.start < a.end:
            return True
        elif a.start <= b.end and b.end <= a.end:
            return True
        return False

    def contains(self, date: datetime.datetime):
        """
        Test if (self.start : self.end] contains date
        """
        return self.start < date and date <= self.end

distant_past = datetime_from_timestamp("0001-01-01T000000.000000Z")
far_future = datetime_from_timestamp("5000-01-01T000000.000000Z")
