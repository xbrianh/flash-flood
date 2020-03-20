FlashFlood is an event recorder and streamer built on top of [AWS S3](https://aws.amazon.com/s3/),
supporting distributed writes and fast distributed bulk reads.

## Installation
    pip install flashflood

## Usage
Instantiate into your favorite bucket and prefix

    import boto3
    from flashflood import FlashFlood
    res = boto3.resource('s3')
    ff = FlashFlood(res, "my_bucket", "my_prefix")


Record new events
```
ff.put(event_data, event_uuid, event_date)
```

Journal events
```
ff.journal(minimum_number_of_events=5000)
```

Get an event
```
ff.get_event(my_event_id)
```

Update event data
```
ff.update_event(my_event_id, my_new_event_data)
```

Replay (stream) events
```
for event in ff.replay(from_date=date_a, to_date=date_b):
    my_event_processor(event.data)
```

Replay events from S3 signed urls:
```
url_info = ff.event_urls(from_date=date, to_date=date_b)
for event in flashflood.events_from_urls(url_info, from_date=date_a, to_date=date_b):
    my_event_processor(event.data)
```

## Links
- [FlashFlood on PyPI](https://pypi.org/project/flash-flood/)
