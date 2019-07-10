import io
import datetime
import json
import requests
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import namedtuple

import boto3

from flashflood.util import timestamp_now, datetime_from_timestamp, distant_past, far_future


s3 = boto3.resource("s3")
s3_client = boto3.client("s3")


ID_PART_DELIMITER = "--"
RINDEX_DELIMITER = ID_PART_DELIMITER + "-"


Collation = namedtuple("Collation", "uid manifest body")
Event = namedtuple("Event", "uid timestamp data")


class FlashFlood:
    def __init__(self, bucket, root_prefix):
        self.bucket = s3.Bucket(bucket)
        self.root_prefix = root_prefix
        self._collation_pfx = f"{root_prefix}/collations"
        self._blobs_pfx = f"{root_prefix}/blobs"
        self._new_pfx = f"{root_prefix}/new"
        self._index_pfx = f"{root_prefix}/index"

    def put(self, data, event_id: str=None, timestamp: str=None):
        timestamp = timestamp or timestamp_now()
        event_id = event_id or str(uuid4())
        assert ID_PART_DELIMITER not in timestamp
        assert ID_PART_DELIMITER not in event_id
        collation_id = timestamp + ID_PART_DELIMITER + timestamp
        manifest = dict(collation_id=collation_id,
                        events=[dict(event_id=event_id, timestamp=timestamp, start=0, size=len(data))])
        self._upload_collation(Collation(collation_id, manifest, io.BytesIO(data)))
        self.bucket.Object(f"{self._new_pfx}/{collation_id}").upload_fileobj(io.BytesIO(b""))
        return Event(event_id, timestamp, data)

    def collate(self, number_of_events=10):
        events = list()
        collations_to_delete = list()
        combined_data = b""
        for collation in self._get_new_collation_parts(number_of_events):
            size = sum([i['size'] for i in events])
            for i in collation.manifest['events']:
                i['start'] += size
                events.append(i)
            combined_data += collation.body.read()
            collations_to_delete.append(collation.uid)
        collation_id = events[0]['timestamp'] + ID_PART_DELIMITER + events[-1]['timestamp']
        manifest = dict(collation_id=collation_id, events=events)
        self._upload_collation(Collation(collation_id, manifest, io.BytesIO(combined_data)))
        self._delete_collations(collations_to_delete)

    def events(self, from_date=distant_past, to_date=far_future):
        for item in self.bucket.objects.filter(Prefix=self._collation_pfx):
            collation_id, start_date, end_date = self._collation_info(item.key)
            if start_date >= from_date and end_date <= to_date:
                collation = self._get_collation(collation_id)
                for i in collation.manifest['events']:
                    part_data = collation.body.read(i['size'])
                    yield Event(i['event_id'], i['timestamp'], part_data)

    def event_urls(self, from_date=distant_past, to_date=far_future):
        urls = list()
        for item in self.bucket.objects.filter(Prefix=self._collation_pfx):
            collation_id, start_date, end_date = self._collation_info(item.key)
            if start_date >= from_date and end_date <= to_date:
                manifest_url = self._generate_presigned_url(collation_id, True)
                collation_url = self._generate_presigned_url(collation_id, False)
                urls.append(dict(manifest=manifest_url, events=collation_url))
        return urls

    def _upload_collation(self, collation):
        key = f"{self._collation_pfx}/{collation.uid}"
        blob_key = f"{self._blobs_pfx}/{collation.uid}"
        self.bucket.Object(blob_key).upload_fileobj(collation.body)
        self.bucket.Object(key).upload_fileobj(io.BytesIO(json.dumps(collation.manifest).encode("utf-8")))

    def _get_collation(self, collation_id, buffered=False):
        key = f"{self._collation_pfx}/{collation_id}"
        blob_key = f"{self._blobs_pfx}/{collation_id}"
        manifest = json.loads(self.bucket.Object(key).get()['Body'].read().decode("utf-8"))
        body = self.bucket.Object(blob_key).get()['Body']
        if buffered:
            body = io.BytesIO(body.read())
        return Collation(collation_id, manifest, body)

    def _get_new_collation_parts(self, number_of_parts):
        collation_items = list()
        for item in self.bucket.objects.filter(Prefix=self._new_pfx):
            collation_items.append(item)
            if len(collation_items) == number_of_parts:
                break
        else:
            raise FlashFloodCollationError(f"Available parts less than {number_of_parts}")
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(self._get_collation, item.key.rsplit("/", 1)[1], True)
                       for item in collation_items]
            collations = [f.result() for f in as_completed(futures)]
        collations.sort(key=lambda collation: collation.uid)
        return collations

    def _generate_presigned_url(self, collation_id, is_manifest):
        if is_manifest:
            key = f"{self._collation_pfx}/{collation_id}"
        else:
            key = f"{self._blobs_pfx}/{collation_id}"
        return s3_client.generate_presigned_url(ClientMethod="get_object",
                                                Params=dict(Bucket=self.bucket.name, Key=key))

    def _collation_info(self, key):
        collation_id = key.rsplit("/", 1)[1]
        start_date, end_date = collation_id.split(ID_PART_DELIMITER)
        return collation_id, datetime_from_timestamp(start_date), datetime_from_timestamp(end_date)

    def _delete_collation(self, collation_id):
        self.bucket.Object(f"{self._collation_pfx}/{collation_id}").delete()
        self.bucket.Object(f"{self._blobs_pfx}/{collation_id}").delete()
        self.bucket.Object(f"{self._new_pfx}/{collation_id}").delete()

    def _delete_collations(self, collation_ids):
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(self._delete_collation, _id) for _id in collation_ids]
            for f in as_completed(futures):
                f.result()

    def _delete_all(self):
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(item.delete)
                       for item in self.bucket.objects.filter(Prefix=self.root_prefix)]
            for f in as_completed(futures):
                f.result()

def events_for_presigned_urls(url_info):
    for urls in url_info:
        resp = requests.get(urls['manifest'])
        resp.raise_for_status()
        manifest = resp.json()
        events_body = requests.get(urls['events'], stream=True).raw
        for item in manifest['events']:
            yield Event(item['event_id'], item['timestamp'], events_body.read(item['size']))

class FlashFloodCollationError(Exception):
    pass
