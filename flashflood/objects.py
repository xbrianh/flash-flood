import io
import json
import typing
from datetime import datetime
from uuid import uuid4
from string import hexdigits
from collections import defaultdict, OrderedDict

from botocore.exceptions import ClientError

from flashflood.util import datetime_from_timestamp, timestamp_now, S3Deleter
from flashflood.exceptions import FlashFloodException, FlashFloodEventNotFound, FlashFloodJournalUploadError
from flashflood.identifiers import JournalID, JournalUpdateID, JournalUpdateAction, TOMBSTONE_SUFFIX


class Event(typing.NamedTuple):
    event_id: str
    date: datetime
    data: bytes


class BaseJournalUpdate:
    bucket: typing.Any = None
    _pfx: typing.Optional[str] = None

    def __init__(self, update_id: JournalUpdateID):
        self.id_ = update_id
        self._data: typing.Optional[bytes] = None

    @classmethod
    def make(cls, journal_id: JournalID, event_id: str, action: JournalUpdateAction):
        id_ = JournalUpdateID.make(journal_id, event_id, action)
        return cls(JournalUpdateID(id_))

    @classmethod
    def from_key(cls, key: str):
        return cls(JournalUpdateID.from_key(key))

    @classmethod
    def upload_update(cls, journal_id: JournalID, event_id: str, data: bytes):
        id_ = JournalUpdateID.make(journal_id, event_id, JournalUpdateAction.UPDATE)
        return cls(id_)._upload(data)

    @classmethod
    def upload_delete(cls, journal_id: JournalID, event_id: str):
        id_ = JournalUpdateID.make(journal_id, event_id, JournalUpdateAction.DELETE)
        return cls(id_)._upload(b"")

    def _upload(self, data: bytes):
        key = f"{self._pfx}/{self.id_}"
        self.bucket.Object(key).upload_fileobj(io.BytesIO(data))

    @property
    def journal_id(self) -> JournalID:
        return self.id_.journal_id

    @property
    def event_id(self) -> str:
        return self.id_.event_id

    @property
    def action(self) -> JournalUpdateAction:
        return self.id_.action

    @property
    def data(self) -> bytes:
        if not self._data:
            key = f"{self._pfx}/{self.id_}"
            try:
                self._data = self.bucket.Object(key).get()['Body'].read()
            except ClientError as ex:
                if ex.response['Error']['Code'] == "NoSuchKey":
                    raise FlashFloodException(f"JournalUpdate not found for key {key}")
                raise
        if self._data is None:
            raise FlashFloodException(f"Update {self.id_} has no data!")
        else:
            return self._data

    def delete(self):
        key = f"{self._pfx}/{self.id_}"
        try:
            next(iter(self.bucket.objects.filter(Prefix=key)))
        except StopIteration:
            raise FlashFloodException(f"Cannot delete non-existent object {key}")
        self.bucket.Object(key=f"{key}{TOMBSTONE_SUFFIX}").upload_fileobj(io.BytesIO(b""))

    @classmethod
    def list_out_of_date_journals(cls) -> typing.Iterator[JournalID]:
        prev_journal_id = str()
        for update_id in cls.list():
            if prev_journal_id != update_id.journal_id:
                yield update_id.journal_id
            prev_journal_id = update_id.journal_id

    @classmethod
    def get_updates_for_all_journals(cls) -> typing.Iterator[typing.Tuple[JournalID, typing.Mapping[str, typing.Any]]]:
        curr_journal_id = JournalID()
        updates: typing.Dict[str, typing.Any] = dict()
        for id_ in cls.list():
            if curr_journal_id != id_.journal_id:
                if updates:
                    yield curr_journal_id, updates
                curr_journal_id = id_.journal_id
                updates = {id_.event_id: cls(id_)}
            else:
                updates[id_.event_id] = cls(id_)
        if updates:
            yield curr_journal_id, updates

    @classmethod
    def get_updates_for_journal(cls, journal_id: JournalID) -> dict:
        update_id_pfx = JournalUpdateID.prefix_for_journal(journal_id)
        updates = dict()
        for id_ in cls.list(update_id_pfx):
            update = cls(id_)
            updates[update.event_id] = update
        return updates

    @classmethod
    def list(cls, update_id_pfx="") -> typing.Iterator[JournalUpdateID]:
        prev_key = None
        key = None
        for item in cls.bucket.objects.filter(Prefix=f"{cls._pfx}/{update_id_pfx}"):
            key = item.key
            if prev_key:
                if not key.endswith(TOMBSTONE_SUFFIX):
                    if not prev_key.endswith(TOMBSTONE_SUFFIX):
                        yield JournalUpdateID.from_key(prev_key)
            prev_key = key
        if key and not key.endswith(TOMBSTONE_SUFFIX):
            yield JournalUpdateID.from_key(key)


class BaseJournal:
    bucket: typing.Any = None
    _journal_pfx: typing.Optional[str] = None
    _blobs_pfx: typing.Optional[str] = None

    def __init__(self, events: list=None, blob_id: str=None, data: bytes=None, version: str=None):
        self.events = events or list()
        self.blob_id = blob_id or str(uuid4())
        self.data = data or b""
        self._body: typing.Optional[typing.BinaryIO] = None
        self._location = "memory"
        self.version = version or timestamp_now()

    @classmethod
    def from_key(cls, key: str):
        id_ = JournalID.from_key(key)
        blob_id = id_.blob_id
        try:
            manifest = json.loads(cls.bucket.Object(key).get()['Body'].read().decode("utf-8"))
        except ClientError as ex:
            if ex.response['Error']['Code'] == "NoSuchKey":
                raise FlashFloodException(f"Journal not found for key {key}")
            else:
                raise
        except json.decoder.JSONDecodeError:
            print("Unable to decode manifest document from key:", key)
            raise
        journal = cls(manifest['events'], blob_id, version=id_.version)
        journal._location = "cloud"
        return journal

    @classmethod
    def from_id(cls, journal_id: JournalID):
        return cls.from_key(f"{cls._journal_pfx}/{journal_id}")

    @property
    def body(self) -> typing.BinaryIO:
        if self._body is None:
            if "memory" == self._location:
                self._body = io.BytesIO(self.data)
            elif "cloud" == self._location:
                key = f"{self._blobs_pfx}/{self.blob_id}"
                self._body = self.bucket.Object(key).get()['Body']
            else:
                raise ValueError(f"Unknown data location {self._location}")
        return self._body

    def reload(self):
        self._body = None

    @property
    def is_empty(self) -> bool:
        return 0 == len(self.events)

    @property
    def id_(self) -> JournalID:
        if self.is_empty:
            raise FlashFloodException("Cannot generate id for empty journal")
        else:
            from_timestamp = self.events[0]['timestamp']
            to_timestamp = self.events[-1]['timestamp']
            return JournalID.make(from_timestamp, to_timestamp, self.version, self.blob_id)

    @property
    def size(self):
        if "memory" == self._location:
            return len(self.data)
        elif "cloud" == self._location:
            return sum(e['size'] for e in self.events)
        else:
            raise ValueError(f"Unknown data location {self._location}")

    def manifest(self) -> dict:
        return dict(journal_id=self.id_,
                    from_date=self.events[0]['timestamp'],
                    to_date=self.events[-1]['timestamp'],
                    size=self.size,
                    events=self.events)

    def get_event(self, event_id: str) -> Event:
        for e in self.events:
            if e['event_id'] == event_id:
                blob_key = f"{self._blobs_pfx}/{self.blob_id}"
                byte_range = f"bytes={e['offset']}-{e['offset'] + e['size'] - 1}"
                data = self.bucket.Object(blob_key).get(Range=byte_range)['Body'].read()
                return Event(event_id, datetime_from_timestamp(e['timestamp']), data)
        raise FlashFloodEventNotFound(f"Event {event_id} not found in journal {self.id_}")

    def updated(self, updates: typing.Mapping[str, BaseJournalUpdate]):
        if not updates:
            return self
        else:
            self.reload()
            new_journal_data = b""
            new_events = list()
            for e in self.events:
                event_data = self.body.read(e['size'])
                e['offset'] = len(new_journal_data)
                update = updates.get(e['event_id'], None)
                if update is None:
                    new_journal_data += event_data
                    new_events.append(e)
                elif JournalUpdateAction.UPDATE == update.action:
                    new_journal_data += update.data
                    e['size'] = len(update.data)
                    new_events.append(e)
                elif JournalUpdateAction.DELETE == update.action:
                    pass
                else:
                    raise Exception("No handler for journal update {update}")
            return type(self)(new_events, data=new_journal_data)

    def upload(self) -> str:
        if self.events:
            manifest = self.manifest()
            blob_key = f"{self._blobs_pfx}/{self.blob_id}"
            self.bucket.Object(blob_key).upload_fileobj(self.body, ExtraArgs=dict(Metadata=dict(journal_id=self.id_)))
            key = f"{self._journal_pfx}/{self.id_}"
            metadata = dict(number_of_events=f"{len(self.events)}",
                            journal_data_size=f"{len(self.data)}")
            self.bucket.Object(key).upload_fileobj(io.BytesIO(json.dumps(manifest).encode("utf-8")),
                                                   ExtraArgs=dict(Metadata=metadata))
            self.reload()  # make self._body available to for read again
            print("Uploaded journal", self.id_)
        else:
            raise FlashFloodJournalUploadError("Cannot upload journal with no events")
        return key

    def delete(self):
        key = f"{self._journal_pfx}/{self.id_}"
        try:
            next(iter(self.bucket.objects.filter(Prefix=key)))
        except StopIteration:
            raise FlashFloodException(f"Cannot delete non-existent object {key}")
        self.bucket.Object(key=f"{key}{TOMBSTONE_SUFFIX}").upload_fileobj(io.BytesIO(b""))

    def keys(self):
        id_ = self.id_
        return [f"{self._journal_pfx}/{id_}", f"{self._blobs_pfx}/{id_.blob_id}"]

    @classmethod
    def list(cls) -> typing.Iterator[JournalID]:
        # TODO: heuristic to find from_date in bucket listing -xbrianh
        journal_info: dict = dict(range_prefix=None, journal_ids=list())
        for item in cls.bucket.objects.filter(Prefix=cls._journal_pfx):
            journal_id = JournalID.from_key(item.key)
            if journal_id.range_prefix != journal_info['range_prefix']:
                if journal_info['journal_ids']:
                    yield journal_info['journal_ids'][-1]
                journal_info['range_prefix'] = journal_id.range_prefix
                journal_info['journal_ids'] = [journal_id]
            else:
                if journal_id.endswith(TOMBSTONE_SUFFIX):
                    journal_info['journal_ids'].remove(journal_id.replace(TOMBSTONE_SUFFIX, ""))
                else:
                    journal_info['journal_ids'].append(journal_id)
        if journal_info['journal_ids']:
            yield journal_info['journal_ids'][-1]
