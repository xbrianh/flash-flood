import datetime

def datetime_to_timestamp(dt):
    return dt.strftime("%Y-%m-%dT%H%M%S.%fZ")

def datetime_from_timestamp(ts):
    return datetime.datetime.strptime(ts, "%Y-%m-%dT%H%M%S.%fZ")

distant_past = datetime_from_timestamp("0001-01-01T000000.000000Z")
far_future = datetime_from_timestamp("5000-01-01T000000.000000Z")
