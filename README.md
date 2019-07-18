FlashFlood is an event recorder and streamer built on top of [AWS S3](https://aws.amazon.com/s3/),
supporting distributed writes and fast distributed bulk reads.

## Installation
    pip install flashflood

## Usage
Instantiate into your favorite bucket and prefix
```
from flashflood import FlashFlood
ff = FlashFlood("my_bucket", "my_prefix")
```

Record new events
```
ff.put(event_data, event_uuid, event_date)
```

Journal events
```
ff.journal(number_of_events=5000)
```

Get an event
```
ff.get_event(my_event_id)
```

Update event data
```
ff.update_event(my_new_event_data, my_event_id)
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
