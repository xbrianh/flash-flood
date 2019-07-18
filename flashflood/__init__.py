import io
from datetime import datetime
import json
import requests
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import namedtuple

import boto3

from flashflood.util import datetime_to_timestamp, datetime_from_timestamp, DateRange


s3 = boto3.resource("s3")
s3_client = boto3.client("s3")


Event = namedtuple("Event", "event_id date data")
_Journal = namedtuple("_Journal", "id_ manifest body")

class _JournalID(str):
    DELIMITER = "--"

    @classmethod
    def make(cls, start_timestamp, end_timestamp, blob_id):
        end_timestamp = end_timestamp or "new"
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
        end_date = self._parts()[1]
        if "new" == end_date:
            return self.start_date
        else:
            return datetime_from_timestamp(end_date)

class FlashFlood:
    def __init__(self, bucket, root_prefix):
        self.bucket = s3.Bucket(bucket)
        self.root_prefix = root_prefix
        self._journal_pfx = f"{root_prefix}/journals"
        self._blobs_pfx = f"{root_prefix}/blobs"
        self._new_pfx = f"{root_prefix}/new"
        self._index_pfx = f"{root_prefix}/index"

    def put(self, data, event_id: str=None, date: datetime=None):
        date = date or datetime.utcnow()
        timestamp = datetime_to_timestamp(date)
        event_id = event_id or str(uuid4())
        assert _JournalID.DELIMITER not in event_id
        blob_id = str(uuid4())
        journal_id = _JournalID.make(timestamp, None, blob_id)
        manifest = dict(journal_id=journal_id,
                        from_date=timestamp,
                        to_date=timestamp,
                        size=len(data),
                        events=[dict(event_id=event_id, timestamp=timestamp, start=0, size=len(data))])
        self._upload_journal(_Journal(journal_id, manifest, io.BytesIO(data)), is_new=True)
        return Event(event_id, date, data)

    def journal(self, number_of_events=10):
        events = list()
        journals_to_delete = list()
        combined_data = b""
        for journal in self._get_new_journals(number_of_events):
            for i in journal.manifest['events']:
                events.append({**i, **dict(start=len(combined_data))})
            combined_data += journal.body.read()
            journals_to_delete.append(journal.id_)
        blob_id = str(uuid4())
        journal_id = _JournalID.make(events[0]['timestamp'], events[-1]['timestamp'], blob_id)
        manifest = dict(journal_id=journal_id,
                        from_date=events[0]['timestamp'],
                        to_date=events[-1]['timestamp'],
                        size=len(combined_data),
                        events=events)
        self._upload_journal(_Journal(journal_id, manifest, io.BytesIO(combined_data)))
        self._delete_journals(journals_to_delete)
        return manifest

    def replay(self, from_date=None, to_date=None):
        search_range = DateRange(from_date, to_date)
        for journal_id in self._journal_ids(from_date, to_date):
            journal = self._get_journal(journal_id)
            for item in journal.manifest['events']:
                event_date = datetime_from_timestamp(item['timestamp'])
                if event_date in search_range:
                    yield Event(item['event_id'], event_date, journal.body.read(item['size']))
                elif event_date in search_range.future:
                    break

    def _lookup_event(self, event_id):
        try:
            key = next(iter(self.bucket.objects.filter(Prefix=f"{self._index_pfx}/{event_id}"))).key
        except StopIteration:
            raise FlashFloodEventNotFound()
        journal_id = _JournalID(self.bucket.Object(key).metadata['journal_id'])
        manifest = self._get_manifest(journal_id)
        for item in manifest['events']:
            if event_id == item['event_id']:
                break
        else:
            raise FlashFloodException(f"Event {event_id} not found in {journal_id}")
        return journal_id, manifest, item

    def get_event(self, event_id):
        journal_id, manifest, item = self._lookup_event(event_id)
        blob_key = f"{self._blobs_pfx}/{journal_id.blob_id}"
        byte_range = f"bytes={item['start']}-{item['start'] + item['size'] - 1}"
        data = self.bucket.Object(blob_key).get(Range=byte_range)['Body'].read()
        return Event(event_id, datetime_from_timestamp(item['timestamp']), data)

    def update_event(self, new_event_data, event_id):
        journal_id, manifest, item = self._lookup_event(event_id)
        blob_key = f"{self._blobs_pfx}/{journal_id.blob_id}"
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
        manifest = dict(journal_id=journal_id,
                        from_date=events[0]['timestamp'],
                        to_date=events[-1]['timestamp'],
                        size=len(new_blob_data),
                        events=events)
        self._upload_journal(_Journal(journal_id, manifest, io.BytesIO(new_blob_data)))

    def replay_urls(self, from_date=None, to_date=None, maximum_number_of_results=1):
        urls = list()
        for journal_id in self._journal_ids(from_date, to_date):
            manifest = self._get_manifest(journal_id)
            journal_url = self._generate_presigned_url(journal_id)
            urls.append(dict(manifest=manifest, events=journal_url))
            if len(urls) == maximum_number_of_results:
                break
        return urls

    def _upload_journal(self, journal, is_new=False):
        key = f"{self._journal_pfx}/{journal.id_}"
        blob_key = f"{self._blobs_pfx}/{journal.id_.blob_id}"
        self.bucket.Object(blob_key).upload_fileobj(journal.body,
                                                    ExtraArgs=dict(Metadata=dict(journal_id=journal.id_)))
        self.bucket.Object(key).upload_fileobj(io.BytesIO(json.dumps(journal.manifest).encode("utf-8")))
        if is_new:
            self.bucket.Object(f"{self._new_pfx}/{journal.id_}").upload_fileobj(io.BytesIO(b""))
        for item in journal.manifest['events']:
            key = f"{self._index_pfx}/{item['event_id']}"
            self.bucket.Object(key).upload_fileobj(io.BytesIO(b""),
                                                   ExtraArgs=dict(Metadata=dict(journal_id=journal.id_)))

    def _get_manifest(self, journal_id):
        key = f"{self._journal_pfx}/{journal_id}"
        return json.loads(self.bucket.Object(key).get()['Body'].read().decode("utf-8"))

    def _get_journal(self, journal_id, buffered=False):
        key = f"{self._blobs_pfx}/{journal_id.blob_id}"
        body = self.bucket.Object(key).get()['Body']
        if buffered:
            body = io.BytesIO(body.read())
        return _Journal(journal_id, self._get_manifest(journal_id), body)

    def _get_new_journals(self, number_of_parts):
        journal_ids = list()
        for item in self.bucket.objects.filter(Prefix=self._new_pfx):
            journal_ids.append(_JournalID.from_key(item.key))
            if number_of_parts == len(journal_ids):
                break
        else:
            raise FlashFloodCollationError(f"Available parts ({len(journal_ids)}) less than {number_of_parts}")
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(self._get_journal, journal_id, buffered=True)
                       for journal_id in journal_ids]
            journals = [f.result() for f in as_completed(futures)]
        journals.sort(key=lambda journal: journal.id_)
        return journals

    def _generate_presigned_url(self, journal_id):
        key = f"{self._blobs_pfx}/{journal_id.blob_id}"
        return s3_client.generate_presigned_url(ClientMethod="get_object",
                                                Params=dict(Bucket=self.bucket.name, Key=key))

    def _delete_journal(self, journal_id):
        self.bucket.Object(f"{self._journal_pfx}/{journal_id}").delete()
        self.bucket.Object(f"{self._blobs_pfx}/{journal_id.blob_id}").delete()
        self.bucket.Object(f"{self._new_pfx}/{journal_id}").delete()

    def _delete_journals(self, journal_ids):
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(self._delete_journal, _id) for _id in journal_ids]
            for f in as_completed(futures):
                f.result()

    def _journal_ids(self, from_date=None, to_date=None):
        # TODO: heuristic to find from_date in bucket listing -xbrianh
        search_range = DateRange(from_date, to_date)
        for item in self.bucket.objects.filter(Prefix=self._journal_pfx):
            journal_id = _JournalID.from_key(item.key)
            journal_range = DateRange(journal_id.start_date, journal_id.end_date)
            if journal_range in search_range:
                yield journal_id
            elif journal_id.start_date in search_range.future:
                break

    def _delete_all_journals(self):
        journal_ids = [journal_id
                       for journal_id in self._journal_ids()]
        self._delete_journals(journal_ids)

    def _destroy(self):
        for item in self.bucket.objects.filter(Prefix=self.root_prefix):
            item.delete()

def replay_with_urls(url_info, from_date=None, to_date=None):
    search_range = DateRange(from_date, to_date)
    for urls in url_info:
        manifest = urls['manifest']
        for event_info in manifest['events']:
            if datetime_from_timestamp(event_info['timestamp']) in search_range:
                break
        byte_range = f"bytes={event_info['start']}-{manifest['size']-1}"
        resp = requests.get(urls['events'], headers=dict(Range=byte_range), stream=True)
        resp.raise_for_status()
        for item in manifest['events']:
            event_date = datetime_from_timestamp(item['timestamp'])
            if event_date in search_range:
                yield Event(item['event_id'], event_date, resp.raw.read(item['size']))
            elif event_date in search_range.future:
                break

class FlashFloodException(Exception):
    pass

class FlashFloodCollationError(FlashFloodException):
    pass

class FlashFloodEventNotFound(FlashFloodException):
    pass
