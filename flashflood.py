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
        self._new_pfx = f"{root_prefix}/new"

    def put(self, data, event_id: str=None, timestamp: str=None):
        timestamp = timestamp or timestamp_now()
        event_id = event_id or str(uuid4())
        assert ID_PART_DELIMITER not in timestamp
        assert ID_PART_DELIMITER not in event_id
        collation_id = timestamp + ID_PART_DELIMITER + timestamp
        manifest = [dict(event_id=event_id, timestamp=timestamp, start=0, size=len(data))]
        self.bucket.Object(f"{self._collation_pfx}/{collation_id}").upload_fileobj(io.BytesIO(data))
        manifest_data = json.dumps(manifest).encode("utf-8")
        self.bucket.Object(f"{self._collation_pfx}/{collation_id}.manifest").upload_fileobj(io.BytesIO(manifest_data))
        self.bucket.Object(f"{self._new_pfx}/{collation_id}").upload_fileobj(io.BytesIO(b""))

    def collate(self, minimum_number_of_events=10):
        manifest = list()
        items_to_delete = list()
        data = b""
        for collation_id, part_manifest, part_data in self._get_new_collation_parts(minimum_number_of_events):
            size = sum([i['size'] for i in manifest])
            for i in part_manifest:
                i['start'] += size
                manifest.append(i)
            data += part_data
            key = f"{self._collation_pfx}/{collation_id}"
            items_to_delete.append(s3.ObjectSummary(self.bucket.name, key))
            items_to_delete.append(s3.ObjectSummary(self.bucket.name, key + ".manifest"))
            items_to_delete.append(s3.ObjectSummary(self.bucket.name, f"{self._new_pfx}/{collation_id}"))
        collation_id = manifest[0]['timestamp'] + ID_PART_DELIMITER + manifest[-1]['timestamp']
        self.bucket.Object(f"{self._collation_pfx}/{collation_id}").upload_fileobj(io.BytesIO(data))
        manifest_data = json.dumps(manifest).encode("utf-8")
        self.bucket.Object(f"{self._collation_pfx}/{collation_id}.manifest").upload_fileobj(io.BytesIO(manifest_data))
        _delete_items(items_to_delete)

    def _get_new_collation_parts(self, number_of_parts):
        def _get_part(item):
            collation_id = item.key.rsplit("/", 1)[1]
            key = f"{self._collation_pfx}/{collation_id}"
            manifest = json.loads(self.bucket.Object(key + ".manifest").get()['Body'].read().decode("utf-8"))
            data = self.bucket.Object(key).get()['Body'].read()
            return collation_id, manifest, data

        collation_items = list()
        for item in self.bucket.objects.filter(Prefix=self._new_pfx):
            if not item.key.endswith(".manifest"):
                collation_items.append(item)
                if len(collation_items) == number_of_parts:
                    break
        else:
            raise FlashFloodCollationError(f"Available parts less than {number_of_parts}")
        with ThreadPoolExecutor(max_workers=10) as e:
            resp = [f.result() for f in as_completed([e.submit(_get_part, item) for item in collation_items])]
        resp.sort(key=lambda x: x[0])
        return resp

    def _collation_info(self, key):
        collation_id = key.rsplit("/", 1)[1]
        start_date, end_date = collation_id.split(ID_PART_DELIMITER)
        return collation_id, datetime_from_timestamp(start_date), datetime_from_timestamp(end_date)

    def events(self, from_date=distant_past, to_date=far_future):
        for collation_blob in self.bucket.objects.filter(Prefix=self._collation_pfx):
            if not collation_blob.key.endswith(".manifest"):
                collation_id, start_date, end_date = self._collation_info(collation_blob.key)
                if start_date >= from_date and end_date <= to_date:
                    manifest = json.loads(self.bucket.Object(collation_blob.key + ".manifest").get()['Body'].read())
                    data = collation_blob.get()['Body']
                    for i in manifest:
                        part_data = data.read(i['size'])
                        yield i['timestamp'], i['event_id'], part_data

    def get_presigned_event_urls(self, from_date=distant_past, to_date=far_future):
        urls = list()
        for collation_blob in self.bucket.objects.filter(Prefix=self._collation_pfx):
            if not collation_blob.key.endswith(".manifest"):
                collation_id, start_date, end_date = self._collation_info(collation_blob.key)
                if start_date >= from_date and end_date <= to_date:
                    collation_key = f"{self._collation_pfx}/{collation_id}"
                    params = dict(Bucket=self.bucket.name, Key=collation_key + ".manifest")
                    manifest_url = s3_client.generate_presigned_url(ClientMethod="get_object",
                                                                 Params=params)
                    params['Key'] = collation_key
                    collation_url = s3_client.generate_presigned_url(ClientMethod="get_object",
                                                                     Params=params)
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
        for item in manifest:
            yield item['timestamp'], item['event_id'], events_body.read(item['size'])

def _delete_items(items):
    with ThreadPoolExecutor(max_workers=15) as e:
        for f in as_completed([e.submit(item.delete) for item in items]):
            f.result()
