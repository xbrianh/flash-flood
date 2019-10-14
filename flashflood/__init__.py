import io
from datetime import datetime
import json
import typing
import requests
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, as_completed

from flashflood.util import datetime_to_timestamp, datetime_from_timestamp, DateRange, S3Deleter
from flashflood.objects import BaseJournal, BaseJournalUpdate
from flashflood.identifiers import JournalID, TOMBSTONE_SUFFIX
from flashflood.event import Event
from flashflood.key_index import BaseKeyIndex
from flashflood.exceptions import FlashFloodEventNotFound, FlashFloodJournalingError


# TODO:
# What happens if event updates are written for a journal that has been removed?
#
# It's possible to resolve journals for update markers that are out-of-date using the event index
#
# Event updates should be applied idempotently
#
# write tests for multiple updates/deletes to the same event
# Create resolution policy for multiple updates/deletes to the same event
#
# Use bucket lifecycle policy for garbage collection
# Warn if policy is not set upon FF instantiation
#
# Include size/len(events) metadat in journal listing?


class FlashFlood:
    def __init__(self, s3_resource: typing.Any, bucket: str, root_prefix: str):
        self.s3 = s3_resource
        self.bucket = self.s3.Bucket(bucket)
        if root_prefix.endswith("/"):
            raise ValueError("Root prefix cannot end with `/`")
        self.root_prefix = root_prefix
        self._journal_pfx = f"{root_prefix}/journals"
        self._blobs_pfx = f"{root_prefix}/blobs"
        self._update_pfx = f"{root_prefix}/update"
        self._index_pfx = f"{root_prefix}/index"

        class _Journal(BaseJournal):
            bucket = self.bucket
            _journal_pfx = self._journal_pfx
            _blobs_pfx = self._blobs_pfx

        class _JournalUpdate(BaseJournalUpdate):
            bucket = self.bucket
            _pfx = self._update_pfx

        class _KeyIndex(BaseKeyIndex):
            bucket = self.bucket
            _pfx = self._index_pfx

        self._Journal = _Journal
        self._JournalUpdate = _JournalUpdate
        self._KeyIndex = _KeyIndex

    def put(self, data, event_id: str=None, date: datetime=None) -> Event:
        """
        Write new event. If event exists, write update marker for event.
        """
        date = date or datetime.utcnow()
        timestamp = datetime_to_timestamp(date)
        event_id = event_id or str(uuid4())
        assert JournalID.DELIMITER not in event_id
        if self.event_exists(event_id):
            journal_id = self._journal_for_event(event_id)
            self._JournalUpdate.upload_update(journal_id, event_id, data)
            return Event(event_id, None, data)
        else:
            events = [dict(event_id=event_id, timestamp=timestamp, offset=0, size=len(data))]
            journal = self._Journal(events, data=data, version="new")
            journal.upload()
            self._index_journal(journal)
            print("new journal", journal.id_)
            return Event(event_id, date, data)

    def _index_journal(self, journal: BaseJournal):
        journal_id = journal.id_
        self._KeyIndex.put_batch({e['event_id']: journal_id for e in journal.events})

    def delete(self, event_id: str):
        """
        Write delete marker for event.
        """
        journal_id = self._journal_for_event(event_id)
        self._JournalUpdate.upload_delete(journal_id, event_id)
        # Deindexing immediately means the event is not available for lookup, but it will still appear in replay.
        self._KeyIndex.delete(event_id)

    def update(self, number_of_updates_to_apply: int=1000) -> int:
        """
        Apply updates for each update and delete marker.
        """
        count = 0
        for journal_id, updates in self._JournalUpdate.get_updates_for_all_journals():
            print("updating journal", journal_id)
            journal = self._Journal.from_id(journal_id)
            new_journal = journal.updated(updates)
            if new_journal != journal:
                if not new_journal.is_empty:
                    new_journal.upload()
                    self._index_journal(new_journal)
                journal.delete()
            for u in updates.values():
                u.delete()
            count += len(updates)
            if number_of_updates_to_apply <= count:
                break
        return count

    def journal(self, minimum_number_of_events: int=100, minimum_size: int=None):
        minimum_size = minimum_size or 0
        number_of_events, size = 0, 0
        journals_to_combine = list()
        for journal_id in self._new_journals():
            journal = self._Journal.from_id(journal_id)
            size += journal.size
            number_of_events += len(journal.events)
            journals_to_combine.append(journal)
            print("Found journal to combine", journal.id_)
            if minimum_number_of_events <= number_of_events and minimum_size <= size:
                break
        if minimum_number_of_events > number_of_events:
            raise FlashFloodJournalingError(f"Journal condition: minimum_number_of_events={minimum_number_of_events}")
        if minimum_size > size:
            raise FlashFloodJournalingError(f"Journal condition: minimum_size={minimum_size}")
        self.combine_journals(journals_to_combine)

    def _new_journals(self) -> typing.Iterator[JournalID]:
        for journal_id in self._Journal.list():
            if "new" == journal_id.version:
                yield journal_id

    def combine_journals(self, journals_to_combine: typing.List[BaseJournal]) -> BaseJournal:
        new_journal = self._Journal()
        objects_to_delete = journals_to_combine.copy()
        for journal in journals_to_combine:
            print("combining journal", journal.id_)
            updates = self._JournalUpdate.get_updates_for_journal(journal.id_)
            objects_to_delete.extend(list(updates.values()))
            journal = journal.updated(updates)
            for e in journal.events:
                new_offset = e['offset'] + len(new_journal.data)
                new_journal.events.append({**e, **dict(offset=new_offset)})
            new_journal.data += journal.body.read()
        if not new_journal.is_empty:
            new_journal.upload()
            self._index_journal(new_journal)
        for o in objects_to_delete:
            o.delete()
        return new_journal

    def replay(self, from_date: datetime=None, to_date: datetime=None) -> typing.Iterator[Event]:
        search_range = DateRange(from_date, to_date)
        for journal_id in self.list_journals(from_date, to_date):
            print("replaying from journal", journal_id)
            journal = self._Journal.from_id(journal_id)
            for item in journal.events:
                event_date = datetime_from_timestamp(item['timestamp'])
                if event_date in search_range:
                    yield Event(item['event_id'], event_date, journal.body.read(item['size']))
                elif event_date in search_range.future:
                    break

    def _journal_for_event(self, event_id: str) -> JournalID:
        journal_id = self._KeyIndex.get(event_id)
        if journal_id is None:
            raise FlashFloodEventNotFound(f"journal not found for {event_id}")
        else:
            return journal_id

    def event_exists(self, event_id: str) -> bool:
        return self._KeyIndex.get(event_id) is not None

    def get_event(self, event_id: str) -> Event:
        journal_id = self._journal_for_event(event_id)
        journal = self._Journal.from_id(journal_id)
        return journal.get_event(event_id)

    def _generate_presigned_url(self, journal_id: JournalID):
        key = f"{self._blobs_pfx}/{journal_id.blob_id}"
        client = self.s3.meta.client
        return client.generate_presigned_url(ClientMethod="get_object",
                                             Params=dict(Bucket=self.bucket.name, Key=key))

    def list_journals(self, from_date: datetime=None, to_date: datetime=None) -> typing.Iterator[JournalID]:
        search_range = DateRange(from_date, to_date)
        for journal_id in self._Journal.list():
            journal_range = DateRange(journal_id.start_date, journal_id.end_date)
            if journal_range in search_range:
                yield journal_id
            elif journal_id.start_date in search_range.future:
                break

    def list_manifests(self,
                       from_date: datetime=None,
                       to_date: datetime=None) -> typing.Iterator[typing.Mapping[str, typing.Any]]:
        for journal_id in self.list_journals(from_date, to_date):
            manifest = self._Journal.from_id(journal_id).manifest()
            manifest['data_url'] = self._generate_presigned_url(journal_id)
            yield manifest

    def _destroy(self):
        with S3Deleter(self.bucket) as s3d:
            for item in self.bucket.objects.filter(Prefix=f"{self.root_prefix}/"):
                s3d.delete(item.key)

def replay_with_manifest(manifest: dict, from_date: datetime=None, to_date: datetime=None) -> typing.Iterator[Event]:
    search_range = DateRange(from_date, to_date)
    for event_info in manifest['events']:
        if datetime_from_timestamp(event_info['timestamp']) in search_range:
            break
    byte_range = f"bytes={event_info['offset']}-{manifest['size']-1}"
    resp = requests.get(manifest['data_url'], headers=dict(Range=byte_range), stream=True)
    resp.raise_for_status()
    for item in manifest['events']:
        event_date = datetime_from_timestamp(item['timestamp'])
        if event_date in search_range:
            yield Event(item['event_id'], event_date, resp.raw.read(item['size']))
        elif event_date in search_range.future:
            break
