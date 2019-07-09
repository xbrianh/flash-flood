import io
import datetime
import json
import requests
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

s3 = boto3.resource("s3")
s3_client = boto3.client("s3")
  
def timestamp_now():
    timestamp = datetime.datetime.utcnow()
    return timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

def datetime_from_timestamp(ts):
    return datetime.datetime.strptime(ts, "%Y-%m-%dT%H%M%S.%fZ")

distant_past = datetime_from_timestamp("0001-01-01T000000.000000Z")
far_future = datetime_from_timestamp("5000-01-01T000000.000000Z")
ID_PART_DELIMITER = "--"
RINDEX_DELIMITER = ID_PART_DELIMITER + "-"

class FlashFloodCollationError(Exception):
    pass

class FlashFlood:
    def __init__(self, bucket, root_prefix):
        self.bucket = s3.Bucket(bucket)
        self.root_prefix = root_prefix
        self._collation_pfx = f"{root_prefix}/collations"
        self._blobs_pfx = f"{root_prefix}/blobs"
        self._new_pfx = f"{root_prefix}/new"

    def put(self, data, event_id: str=None, timestamp: str=None):
        timestamp = timestamp or timestamp_now()
        event_id = event_id or str(uuid4())
        assert ID_PART_DELIMITER not in timestamp
        assert ID_PART_DELIMITER not in event_id
        collation_id = timestamp + ID_PART_DELIMITER + timestamp
        manifest = dict(collation_id=collation_id,
                        events=[dict(event_id=event_id, timestamp=timestamp, start=0, size=len(data))])
        self._upload_collation(collation_id, data)
        self._upload_manifest(manifest)
        self.bucket.Object(f"{self._new_pfx}/{collation_id}").upload_fileobj(io.BytesIO(b""))

    def collate(self, minimum_number_of_events=10):
        events = list()
        collations_to_delete = list()
        data = b""
        for collation_id, part_manifest, part_data in self._get_new_collation_parts(minimum_number_of_events):
            size = sum([i['size'] for i in events])
            for i in part_manifest['events']:
                i['start'] += size
                events.append(i)
            data += part_data
            key = f"{self._collation_pfx}/{collation_id}"
            collations_to_delete.append(collation_id)
        collation_id = events[0]['timestamp'] + ID_PART_DELIMITER + events[-1]['timestamp']
        self._upload_collation(collation_id, data)
        self._upload_manifest(dict(collation_id=collation_id, events=events))
        self._delete_collations(collations_to_delete)

    def _upload_collation(self, collation_id, data):
        key = f"{self._blobs_pfx}/{collation_id}"
        self.bucket.Object(f"{self._blobs_pfx}/{collation_id}").upload_fileobj(io.BytesIO(data))

    def _upload_manifest(self, manifest):
        key = f"{self._collation_pfx}/{manifest['collation_id']}"
        data = json.dumps(manifest).encode("utf-8")
        self.bucket.Object(key).upload_fileobj(io.BytesIO(data))

    def _collation_stream(self, collation_id):
        key = f"{self._blobs_pfx}/{collation_id}"
        return self.bucket.Object(key).get()['Body']

    def _manifest(self, collation_id):
        key = f"{self._collation_pfx}/{collation_id}"
        return json.loads(self.bucket.Object(key).get()['Body'].read().decode("utf-8"))

    def _delete_collation(self, collation_id):
        self.bucket.Object(f"{self._collation_pfx}/{collation_id}").delete()
        self.bucket.Object(f"{self._blobs_pfx}/{collation_id}").delete()
        self.bucket.Object(f"{self._new_pfx}/{collation_id}").delete()

    def _delete_collations(self, collation_ids):
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(self._delete_collation, _id) for _id in collation_ids]
            for f in as_completed(futures):
                f.result()

    def _get_new_collation_parts(self, number_of_parts):
        def _get_part(item):
            collation_id = item.key.rsplit("/", 1)[1]
            manifest = self._manifest(collation_id)
            return collation_id, manifest, self._collation_stream(collation_id).read()

        collation_items = list()
        for item in self.bucket.objects.filter(Prefix=self._new_pfx):
            collation_items.append(item)
            if len(collation_items) == number_of_parts:
                break
        else:
            raise FlashFloodCollationError(f"Available parts less than {number_of_parts}")
        with ThreadPoolExecutor(max_workers=10) as e:
            resp = [f.result() for f in as_completed([e.submit(_get_part, item) for item in collation_items])]
        resp.sort(key=lambda x: x[0])
        return resp

    def _generate_presigned_url(self, collation_id, is_manifest):
        if is_manifest:
            key = f"{self._collation_pfx}/{collation_id}"
        else:
            key = f"{self._blobs_pfx}/{collation_id}"
        return s3_client.generate_presigned_url(ClientMethod="get_object",
                                                Params=dict(Bucket=self.bucket.name, Key=key))

    def _collation_url(self, collation_id):
        key = f"{self._collation_pfx}/{collation_id}"
        params = dict(Bucket=self.bucket.name, Key=key)
        manifest_url = s3_client.generate_presigned_url(ClientMethod="get_object",
                                                        Params=params)

    def _collation_info(self, key):
        collation_id = key.rsplit("/", 1)[1]
        start_date, end_date = collation_id.split(ID_PART_DELIMITER)
        return collation_id, datetime_from_timestamp(start_date), datetime_from_timestamp(end_date)

    def events(self, from_date=distant_past, to_date=far_future):
        for manifest_blob in self.bucket.objects.filter(Prefix=self._collation_pfx):
            collation_id, start_date, end_date = self._collation_info(manifest_blob.key)
            if start_date >= from_date and end_date <= to_date:
                manifest = self._manifest(collation_id)
                collation = self._collation_stream(collation_id)
                for i in manifest['events']:
                    part_data = collation.read(i['size'])
                    yield i['timestamp'], i['event_id'], part_data

    def get_presigned_event_urls(self, from_date=distant_past, to_date=far_future):
        urls = list()
        for collation_blob in self.bucket.objects.filter(Prefix=self._collation_pfx):
            collation_id, start_date, end_date = self._collation_info(collation_blob.key)
            if start_date >= from_date and end_date <= to_date:
                manifest_url = self._generate_presigned_url(collation_id, True)
                collation_url = self._generate_presigned_url(collation_id, False)
                urls.append(dict(manifest=manifest_url, events=collation_url))
        return urls

    def _delete_all(self):
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(blob.delete)
                       for blob in self.bucket.objects.filter(Prefix=self.root_prefix)]
            for f in as_completed(futures):
                f.result()

def events_for_presigned_urls(url_info):
    for urls in url_info:
        resp = requests.get(urls['manifest'])
        resp.raise_for_status()
        manifest = resp.json()
        events_body = requests.get(urls['events'], stream=True).raw
        for item in manifest['events']:
            yield item['timestamp'], item['event_id'], events_body.read(item['size'])
