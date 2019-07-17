import datetime
from collections import namedtuple

def datetime_to_timestamp(dt):
    return dt.strftime("%Y-%m-%dT%H%M%S.%fZ")

def datetime_from_timestamp(ts):
    return datetime.datetime.strptime(ts, "%Y-%m-%dT%H%M%S.%fZ")

class DateRange:
    def __init__(self, start: datetime.datetime=None, end: datetime.datetime=None):
        self.start = start or datetime.datetime.min
        self.end = end or datetime.datetime.max
        assert self.start <= self.end

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

    def __contains__(self, item: datetime.datetime):
        if isinstance(item, datetime.datetime):
            return self.contains(item)
        elif isinstance(item, type(self)):
            return item.overlaps(self)
        else:
            raise TypeError(f"expected datetime instance or {type(self)} instance")

    @property
    def past(self):
        if datetime.datetime.min == self.start:
            return _EmptyDateRange()
        else:
            return type(self)(datetime.datetime.max, self.start)

    @property
    def future(self):
        if datetime.datetime.max == self.end:
            return _EmptyDateRange()
        else:
            return type(self)(self.end, datetime.datetime.max)

class _EmptyDateRange(DateRange):
    """
    This class represents an empty date range
    """
    def overlaps(self, *args, **kwargs):
        return False

    def contains(self, *args, **kwargs):
        return False
