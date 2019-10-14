from uuid import uuid4
from enum import Enum
from functools import lru_cache

from flashflood.util import datetime_from_timestamp, timestamp_now


TOMBSTONE_SUFFIX = ".dead"


class JournalUpdateAction(Enum):
    UPDATE = "update"
    DELETE = "delete"


class JournalID(str):
    DELIMITER = "--"

    @classmethod
    def make(cls, start_timestamp: str, end_timestamp: str, version: str, blob_id: str):
        return cls(start_timestamp + cls.DELIMITER + end_timestamp + cls.DELIMITER + version + cls.DELIMITER + blob_id)

    @classmethod
    def from_key(cls, key):
        return cls(key.rsplit("/", 1)[1])

    @lru_cache()
    def _parts(self):
        start_timestamp, end_timestamp, version, blob_id = self.split(self.DELIMITER)
        return start_timestamp, end_timestamp, version, blob_id

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

    @property
    def version(self) -> str:
        return self._parts()[2]

    @property
    def blob_id(self):
        return self._parts()[3]

    @property
    def range_prefix(self):
        return self.rsplit(self.DELIMITER, 2)[0]


class JournalUpdateID(str):
    """
    This defines the id used to compose the object key on storage for journal updates.
    """
    DELIMITER = "--"

    @classmethod
    def make(cls, journal_id: str, event_id: str, action: JournalUpdateAction):
        reverse_journal_id = journal_id[::-1]
        return cls(reverse_journal_id + cls.DELIMITER
                   + event_id + cls.DELIMITER
                   + timestamp_now() + cls.DELIMITER
                   + action.name)

    @classmethod
    def from_key(cls, key):
        return cls(key.rsplit("/", 1)[1])

    @lru_cache()
    def _parts(self):
        reverse_journal_id, event_id, timestamp, action_name = self.rsplit(JournalID.DELIMITER, 3)
        return JournalID(reverse_journal_id[::-1]), event_id, timestamp, JournalUpdateAction[action_name]

    @property
    def journal_id(self) -> JournalID:
        return self._parts()[0]

    @property
    def event_id(self) -> str:
        return self._parts()[1]

    @property
    def timestamp(self) -> str:
        return self._parts()[2]

    @property
    def action(self) -> JournalUpdateAction:
        return self._parts()[3]

    @staticmethod
    def prefix_for_journal(journal_id: JournalID):
        return journal_id[::-1]
