import io
from datetime import datetime
import json
import requests
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import namedtuple

import boto3

from flashflood.util import datetime_to_timestamp, datetime_from_timestamp, distant_past, far_future


s3 = boto3.resource("s3")
s3_client = boto3.client("s3")


Collation = namedtuple("_Collation", "uid manifest body")
Event = namedtuple("Event", "uid date data")

class _CollationID(str):
    DELIMITER = "--"

    @classmethod
    def make(cls, start_timestamp, end_timestamp, blob_id):
        return cls(start_timestamp + cls.DELIMITER + end_timestamp + cls.DELIMITER + blob_id)

    @classmethod
    def from_key(cls, key):
        return cls(key.rsplit("/", 1)[1])

    def _parts(self):
        start_timestamp, end_timestamp, blob_id = self.split(self.DELIMITER)
        return start_timestamp, end_timestamp, blob_id

    @property
    def blob_id(self):
        return self._parts()[2]

    @property
    def start_date(self):
        return datetime_from_timestamp(self._parts()[0])

    @property
    def end_date(self):
        return datetime_from_timestamp(self._parts()[1])

class FlashFlood:
    def __init__(self, bucket, root_prefix):
        self.bucket = s3.Bucket(bucket)
        self.root_prefix = root_prefix
        self._collation_pfx = f"{root_prefix}/collations"
        self._blobs_pfx = f"{root_prefix}/blobs"
        self._new_pfx = f"{root_prefix}/new"
        self._index_pfx = f"{root_prefix}/index"

    def put(self, data, event_id: str=None, date: datetime=None):
        date = date or datetime.utcnow()
        timestamp = datetime_to_timestamp(date)
        event_id = event_id or str(uuid4())
        assert _CollationID.DELIMITER not in event_id
        blob_id = str(uuid4())
        collation_id = _CollationID.make(timestamp, "new", blob_id)
        manifest = dict(collation_id=collation_id,
                        from_date=timestamp,
                        to_date=timestamp,
                        size=len(data),
                        events=[dict(event_id=event_id, timestamp=timestamp, start=0, size=len(data))])
        self._upload_collation(Collation(collation_id, manifest, io.BytesIO(data)), is_new=True)
        return Event(event_id, date, data)

    def collate(self, number_of_events=10):
        events = list()
        collations_to_delete = list()
        combined_data = b""
        for collation in self._get_new_collations(number_of_events):
            for i in collation.manifest['events']:
                events.append({**i, **dict(start=len(combined_data))})
            combined_data += collation.body.read()
            collations_to_delete.append(collation.uid)
        blob_id = str(uuid4())
        collation_id = _CollationID.make(events[0]['timestamp'], events[-1]['timestamp'], blob_id)
        manifest = dict(collation_id=collation_id,
                        from_date=events[0]['timestamp'],
                        to_date=events[-1]['timestamp'],
                        size=len(combined_data),
                        events=events)
        self._upload_collation(Collation(collation_id, manifest, io.BytesIO(combined_data)))
        self._delete_collations(collations_to_delete)
        return manifest

    def events(self, from_date=None):
        event_from_date = from_date or distant_past
        for collation_id in self._collation_ids(from_date):
            collation = self._get_collation(collation_id)
            for i in collation.manifest['events']:
                event_date = datetime_from_timestamp(i['timestamp'])
                if event_date > event_from_date:
                    date = datetime_from_timestamp(i['timestamp'])
                    yield Event(i['event_id'], date, collation.body.read(i['size']))

    def _lookup_event(self, event_id):
        try:
            key = next(iter(self.bucket.objects.filter(Prefix=f"{self._index_pfx}/{event_id}"))).key
        except StopIteration:
            raise FlashFloodEventNotFound()
        collation_id = _CollationID(self.bucket.Object(key).metadata['collation_id'])
        manifest = self._get_manifest(collation_id)
        for item in manifest['events']:
            if event_id == item['event_id']:
                break
        else:
            raise FlashFloodException(f"Event {event_id} not found in {collation_id}")
        return collation_id, manifest, item

    def get_event(self, event_id):
        collation_id, manifest, item = self._lookup_event(event_id)
        blob_key = f"{self._blobs_pfx}/{collation_id.blob_id}"
        byte_range = f"bytes={item['start']}-{item['start'] + item['size'] - 1}"
        data = self.bucket.Object(blob_key).get(Range=byte_range)['Body'].read()
        return Event(event_id, datetime_from_timestamp(item['timestamp']), data)

    def update_event(self, new_event_data, event_id):
        collation_id, manifest, item = self._lookup_event(event_id)
        blob_key = f"{self._blobs_pfx}/{collation_id.blob_id}"
        blob_data = self.bucket.Object(blob_key).get()['Body'].read()
        new_blob_data = (blob_data[:item['start']]
                         + new_event_data
                         + blob_data[item['start'] + item['size']:])
        item['size'] = len(new_event_data)
        data_size = 0
        for item in manifest['events']:
            item['start'] = data_size
            data_size += item['size']
        events = manifest['events']
        manifest = dict(collation_id=collation_id,
                        from_date=events[0]['timestamp'],
                        to_date=events[-1]['timestamp'],
                        size=len(new_blob_data),
                        events=events)
        self._upload_collation(Collation(collation_id, manifest, io.BytesIO(new_blob_data)))

    def event_urls(self, from_date=None, number_of_pages=1):
        urls = list()
        for collation_id in self._collation_ids(from_date):
            manifest = self._get_manifest(collation_id)
            collation_url = self._generate_presigned_url(collation_id)
            urls.append(dict(manifest=manifest, events=collation_url))
            if len(urls) == number_of_pages:
                break
        return urls

    def _upload_collation(self, collation, is_new=False):
        key = f"{self._collation_pfx}/{collation.uid}"
        blob_key = f"{self._blobs_pfx}/{collation.uid.blob_id}"
        self.bucket.Object(blob_key).upload_fileobj(collation.body,
                                                    ExtraArgs=dict(Metadata=dict(collation_id=collation.uid)))
        self.bucket.Object(key).upload_fileobj(io.BytesIO(json.dumps(collation.manifest).encode("utf-8")))
        if is_new:
            self.bucket.Object(f"{self._new_pfx}/{collation.uid}").upload_fileobj(io.BytesIO(b""))
        for item in collation.manifest['events']:
            key = f"{self._index_pfx}/{item['event_id']}"
            self.bucket.Object(key).upload_fileobj(io.BytesIO(b""),
                                                   ExtraArgs=dict(Metadata=dict(collation_id=collation.uid)))

    def _get_manifest(self, collation_id):
        key = f"{self._collation_pfx}/{collation_id}"
        return json.loads(self.bucket.Object(key).get()['Body'].read().decode("utf-8"))

    def _get_collation(self, collation_id, buffered=False):
        key = f"{self._blobs_pfx}/{collation_id.blob_id}"
        body = self.bucket.Object(key).get()['Body']
        if buffered:
            body = io.BytesIO(body.read())
        return Collation(collation_id, self._get_manifest(collation_id), body)

    def _get_new_collations(self, number_of_parts):
        collation_ids = list()
        for item in self.bucket.objects.filter(Prefix=self._new_pfx):
            collation_ids.append(_CollationID.from_key(item.key))
            if number_of_parts == len(collation_ids):
                break
        else:
            raise FlashFloodCollationError(f"Available parts ({len(collation_ids)}) less than {number_of_parts}")
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(self._get_collation, collation_id, buffered=True)
                       for collation_id in collation_ids]
            collations = [f.result() for f in as_completed(futures)]
        collations.sort(key=lambda collation: collation.uid)
        return collations

    def _generate_presigned_url(self, collation_id):
        key = f"{self._blobs_pfx}/{collation_id.blob_id}"
        return s3_client.generate_presigned_url(ClientMethod="get_object",
                                                Params=dict(Bucket=self.bucket.name, Key=key))

    def _delete_collation(self, collation_id):
        self.bucket.Object(f"{self._collation_pfx}/{collation_id}").delete()
        self.bucket.Object(f"{self._blobs_pfx}/{collation_id.blob_id}").delete()
        self.bucket.Object(f"{self._new_pfx}/{collation_id}").delete()

    def _delete_collations(self, collation_ids):
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(self._delete_collation, _id) for _id in collation_ids]
            for f in as_completed(futures):
                f.result()

    def _collation_ids(self, from_date=None):
        for item in self.bucket.objects.filter(Prefix=self._collation_pfx):
            collation_id = _CollationID.from_key(item.key)
            if from_date:
                if collation_id.end_date <= from_date:
                    continue
            yield collation_id

    def _delete_all_collations(self):
        collation_ids = [collation_id for collation_id in self._collation_ids()]
        self._delete_collations(collation_ids)

    def _destroy(self):
        for item in self.bucket.objects.filter(Prefix=self.root_prefix):
            item.delete()

def events_from_urls(url_info, from_date=distant_past):
    for urls in url_info:
        manifest = urls['manifest']
        start_byte = 0
        for event_info in manifest['events']:
            if datetime_from_timestamp(event_info['timestamp']) > from_date:
                break
            else:
                start_byte += event_info['size']
        else:
            continue
        byte_range = f"bytes={start_byte}-{manifest['size']-1}"
        resp = requests.get(urls['events'], headers=dict(Range=byte_range), stream=True)
        resp.raise_for_status()
        for item in manifest['events']:
            event_date = datetime_from_timestamp(item['timestamp'])
            if event_date > from_date:
                yield Event(item['event_id'], event_date, resp.raw.read(item['size']))

class FlashFloodException(Exception):
    pass

class FlashFloodCollationError(FlashFloodException):
    pass

class FlashFloodEventNotFound(FlashFloodException):
    pass
