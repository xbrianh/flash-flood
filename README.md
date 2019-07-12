FlashFlood is an event recorder and streamer built on top of [AWS S3](https://aws.amazon.com/s3/),
emphasizing scalability and fast bulk reads.

## Installation
    pip install flashflood

## Usage
Instantiate into your favorite bucket and prefix
```
from flashflood import FlashFlood
ff = FlashFlood("my_bucket", "my_prefix")
```

Record events (this method may be called in parallelized or distributed environments)
```
ff.put(event_data, event_uuid, event_date)
```

To optimize bulk reads, call `collate` (this must be executed non-concurrently)
```
ff.collate(number_of_events=5000)
```

Stream events starting from `date`
```
for event in ff.events(from_date=date):
	my_event_processor(event.data)
```

Stream events from S3 signed urls:
```
url_info = ff.event_urls(from_date=date)
for event in flashflood.events_from_urls(url_info, from_date=date):
	my_event_processor(event.data)
```
