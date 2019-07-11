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
        collation_id = timestamp + ID_PART_DELIMITER + "new"
        manifest = dict(collation_id=collation_id,
                        from_date=timestamp,
                        to_date=timestamp,
                        events=[dict(event_id=event_id, timestamp=timestamp, size=len(data))])
        self._upload_collation(Collation(collation_id, manifest, io.BytesIO(data)))
        self.bucket.Object(f"{self._new_pfx}/{collation_id}").upload_fileobj(io.BytesIO(b""))
        return Event(event_id, timestamp, data)

    def collate(self, number_of_events=10):
        events = list()
        collations_to_delete = list()
        combined_data = b""
        for collation in self._get_new_collations(number_of_events):
            size = sum([i['size'] for i in events])
            for i in collation.manifest['events']:
                events.append(i)
            combined_data += collation.body.read()
            collations_to_delete.append(collation.uid)
        collation_id = events[0]['timestamp'] + ID_PART_DELIMITER + events[-1]['timestamp']
        manifest = dict(collation_id=collation_id,
                        from_date=events[0]['timestamp'],
                        to_date=events[-1]['timestamp'],
                        events=events)
        self._upload_collation(Collation(collation_id, manifest, io.BytesIO(combined_data)))
        self._delete_collations(collations_to_delete)

    def events(self, from_date=None):
        event_from_date = from_date or distant_past
        for collation_id in self._collations_ids(from_date):
            collation = self._get_collation(collation_id)
            for i in collation.manifest['events']:
                event_date = datetime_from_timestamp(i['timestamp'])
                if event_date >= event_from_date:
                    yield Event(i['event_id'], i['timestamp'], collation.body.read(i['size']))

    def event_urls(self, from_date=None, number_of_pages=1):
        urls = list()
        for collation_id in self._collations_ids(from_date):
            manifest = self._get_manifest(collation_id)
            collation_url = self._generate_presigned_url(collation_id, False)
            urls.append(dict(manifest=manifest, events=collation_url))
            if len(urls) == number_of_pages:
                break
        return urls

    def _upload_collation(self, collation):
        key = f"{self._collation_pfx}/{collation.uid}"
        blob_key = f"{self._blobs_pfx}/{collation.uid}"
        self.bucket.Object(blob_key).upload_fileobj(collation.body)
        self.bucket.Object(key).upload_fileobj(io.BytesIO(json.dumps(collation.manifest).encode("utf-8")))

    def _get_manifest(self, collation_id):
        key = f"{self._collation_pfx}/{collation_id}"
        return json.loads(self.bucket.Object(key).get()['Body'].read().decode("utf-8"))

    def _get_collation(self, collation_id, buffered=False):
        key = f"{self._blobs_pfx}/{collation_id}"
        body = self.bucket.Object(key).get()['Body']
        if buffered:
            body = io.BytesIO(body.read())
        return Collation(collation_id, self._get_manifest(collation_id), body)

    def _get_new_collations(self, number_of_parts):
        collation_ids = list()
        for item in self.bucket.objects.filter(Prefix=self._new_pfx):
            collation_ids.append(item.key.rsplit("/", 1)[1])
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

    def _generate_presigned_url(self, collation_id, is_manifest):
        if is_manifest:
            key = f"{self._collation_pfx}/{collation_id}"
        else:
            key = f"{self._blobs_pfx}/{collation_id}"
        return s3_client.generate_presigned_url(ClientMethod="get_object",
                                                Params=dict(Bucket=self.bucket.name, Key=key))

    def _delete_collation(self, collation_id):
        self.bucket.Object(f"{self._collation_pfx}/{collation_id}").delete()
        self.bucket.Object(f"{self._blobs_pfx}/{collation_id}").delete()
        self.bucket.Object(f"{self._new_pfx}/{collation_id}").delete()

    def _delete_collations(self, collation_ids):
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(self._delete_collation, _id) for _id in collation_ids]
            for f in as_completed(futures):
                f.result()

    def _collations_ids(self, from_date=None):
        for item in self.bucket.objects.filter(Prefix=self._collation_pfx):
            collation_id = item.key.rsplit("/", 1)[1]
            if from_date:
                timestamps = collation_id.split("--", 1)
                try:
                    collation_end_date = datetime_from_timestamp(timestamps[1])
                except ValueError:
                    collation_end_date = datetime_from_timestamp(timestamps[0])
                if collation_end_date <= from_date:
                    continue
            yield collation_id

    def _delete_all(self):
        collation_ids = [collation_id for collation_id in self._collations_ids()]
        self._delete_collations(collation_ids)

def events_from_urls(url_info, from_date=distant_past):
    for urls in url_info:
        manifest = urls['manifest']
        resp = requests.get(urls['events'], stream=True)
        resp.raise_for_status()
        for item in manifest['events']:
            if datetime_from_timestamp(item['timestamp']) > from_date:
                yield Event(item['event_id'], item['timestamp'], resp.raw.read(item['size']))

class FlashFloodCollationError(Exception):
    pass
