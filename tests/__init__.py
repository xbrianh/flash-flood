from datetime import datetime
from random import randint

from flashflood import config
from flashflood.util import datetime_from_timestamp

config.object_exists_waiter_config['Delay'] = 1

def random_date() -> datetime:
    year = "%04i" % randint(1000, 2019)
    month = "%02i" % randint(1, 12)
    day = "%02i" % randint(1, 28)
    hours = "%02i" % randint(0, 23)
    minutes = "%02i" % randint(0, 59)
    seconds = "%02i" % randint(0, 59)
    fractions = "%06i" % randint(1, 999999)
    timestamp = f"{year}-{month}-{day}T{hours}{minutes}{seconds}.{fractions}Z"
    return datetime_from_timestamp(timestamp)
