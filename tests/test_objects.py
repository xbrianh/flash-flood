#!/usr/bin/env python
import io
import os
import sys
import time
import typing
from datetime import datetime, timedelta
from uuid import uuid4
from random import randint
from collections import defaultdict
import unittest

import boto3

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from flashflood.objects import BaseJournal, BaseJournalUpdate
from flashflood.identifiers import JournalID, JournalUpdateID, JournalUpdateAction, TOMBSTONE_SUFFIX
from flashflood.util import concurrent_listing, delete_keys, datetime_to_timestamp, timestamp_now
from flashflood.exceptions import FlashFloodException, FlashFloodJournalUploadError
from tests import infra, random_date


class TestObjects(unittest.TestCase):
    bucket: typing.Any = None

    @classmethod
    def setUpClass(cls):
        cls.root_pfx = f"flashflood-test-key-index-{uuid4()}"
        cls.s3 = boto3.resource("s3")
        cls.bucket = cls.s3.Bucket(infra.get_env("S3_BUCKET"))

        class Journal(BaseJournal):
            bucket = cls.bucket
            _journal_pfx = f"{cls.root_pfx}/journals"
            _blob_pfx = f"{cls.root_pfx}/blobs"

        class JournalUpdate(BaseJournalUpdate):
            bucket = cls.bucket
            _pfx = f"{cls.root_pfx}/updates"

        cls.Journal = Journal
        cls.JournalUpdate = JournalUpdate

    @classmethod
    def tearDownClass(cls):
        keys_to_delete = [item.key for item in concurrent_listing(cls.bucket, [f"{cls.root_pfx}/"])]
        delete_keys(cls.bucket, keys_to_delete)

    def setUp(self):
        self.events = list()
        self.event_data = dict()
        self.journal_data = b""
        for _ in range(3):
            event_data = os.urandom(5)
            event_id = str(uuid4())
            self.event_data[event_id] = event_data
            self.events.append(dict(event_id=event_id,
                                    timestamp=timestamp_now(),
                                    offset=len(self.journal_data),
                                    size=len(event_data)))
            self.journal_data += event_data
        self.journal = self.Journal(self.events, data=self.journal_data)

    def test_journal_is_empty(self):
        self.assertFalse(self.journal.is_empty)
        empty_journal = self.Journal()
        self.assertTrue(empty_journal.is_empty)

    def test_journal_version(self):
        new_journal = self.Journal(self.journal.events, data=self.journal.data)
        self.assertEqual(new_journal.id_.start_date, self.journal.id_.start_date)
        self.assertEqual(new_journal.id_.end_date, self.journal.id_.end_date)
        self.assertNotEqual(new_journal.id_.blob_id, self.journal.id_.blob_id)
        self.assertGreater(new_journal.version, self.journal.version)

    def test_journal_body_local(self):
        with self.subTest("Journal should report data in memory"):
            self.assertEqual("memory", self.journal._location)
        with self.subTest("Reading body should return journal data"):
            self.assertEqual(self.journal.body.read(), self.journal_data)
        with self.subTest("Reading closed body should raise"):
            with self.assertRaises(ValueError):
                self.journal.body.close()
                self.journal.body.read()
        with self.subTest("Should be able to read after reload"):
            self.journal.reload()
            self.assertEqual(self.journal.body.read(), self.journal_data)

    def test_journal_body_cloud(self):
        key = self.journal.upload()
        journal = self.Journal.from_key(key)
        with self.subTest("Journal should report data in cloud"):
            self.assertEqual("cloud", journal._location)
        with self.subTest("Reading body should return journal data"):
            self.assertEqual(journal.body.read(), self.journal_data)
        with self.subTest("Reading body again should return empty data"):
            self.assertEqual(journal.body.read(), b"")
        with self.subTest("Should be able to read after reload"):
            journal.reload()
            self.assertEqual(journal.body.read(), self.journal_data)

    def test_journal_upload(self):
        with self.subTest("Should not be able to upload empty journal"):
            with self.assertRaises(FlashFloodJournalUploadError):
                self.Journal().upload()
        with self.subTest("Upload should succeed"):
            self.Journal(self.events, data=self.journal_data).upload()

    def test_journal_delete(self):
        journal = self.Journal.from_key(self.journal.upload())
        with self.subTest("Should be able to delete uploaded journal"):
            journal.delete()
        with self.subTest("Should NOT be able to delete non-uploaded journal"):
            with self.assertRaises(FlashFloodException):
                self.Journal().delete()

    def test_journal_get(self):
        key = self.journal.upload()
        with self.subTest("Should be able to retrieve journal by key"):
            new_journal = self.Journal.from_key(key)
            self.assertEqual(self.journal.version, new_journal.version)
            self.assertEqual(self.journal.data, new_journal.body.read())
        with self.subTest("Should be able to retrieve journal by id"):
            new_journal = self.Journal.from_id(self.journal.id_)
            self.assertEqual(self.journal.version, new_journal.version)
            self.assertEqual(self.journal.data, new_journal.body.read())
        with self.subTest("Should not be able to get non-existent journal"):
            with self.assertRaises(FlashFloodException):
                id_ = JournalID.make("alskdfj", "alskdfj", "sdf", "jfldkja")
                key = f"{self.root_pfx}/journals/{id_}"
                self.Journal.from_key(key)

    def test_journal_get_event(self):
        journal = self.Journal.from_key(self.journal.upload())
        event_id = self.events[1]['event_id']
        for event_info in self.events:
            event_id = event_info['event_id']
            e = journal.get_event(event_id)
            self.assertEqual(e.data, self.event_data[event_id])

    def test_list_journals(self):
        """
        Test that journals are listed omitting old versions and tombstones.
        For example, the following journal ids
        A--version1
        A--version1.dead
        A--version2
        B--version1
        B--version1.dead
        C--version1
        C--version2
        D--version1
        should be listed as:
        A--version2
        C--version2
        D--version1
        """
        pfx = f"{self.root_pfx}/test_list_journals"
        journal_ids = list()
        latest_journal_ids = dict()
        for _ in range(20):
            start_date = random_date()
            end_date = start_date + timedelta(seconds=1)
            for _ in range(randint(1, 5)):
                journal_id = JournalID.make(datetime_to_timestamp(start_date),
                                            datetime_to_timestamp(end_date),
                                            timestamp_now(),
                                            str(uuid4()))
                self.bucket.Object(key=f"{pfx}/{journal_id}").upload_fileobj(io.BytesIO(b""))
                journal_ids.append(journal_id)
                if 1 == randint(1, 4):
                    self.bucket.Object(key=f"{pfx}/{journal_id}{TOMBSTONE_SUFFIX}").upload_fileobj(io.BytesIO(b""))
                    journal_ids.append(f"{journal_id}{TOMBSTONE_SUFFIX}")
                else:
                    latest_journal_ids[journal_id.range_prefix] = journal_id

        class Journal(BaseJournal):
            bucket = self.bucket
            _journal_pfx = pfx
            _blob_pfx = self.Journal._blob_pfx

        with self.subTest("Test listing all Journals"):
            expected_ids = sorted(latest_journal_ids.values())
            self.assertEqual(expected_ids, [id_ for id_ in Journal.list()])

        with self.subTest("Test listing after known journal id"):
            index = randint(1, len(expected_ids) - 2)
            list_from = expected_ids[index]
            self.assertEqual(expected_ids[1 + index:], [id_ for id_ in Journal.list(list_from=list_from)])

    def test_journal_update_properties(self):
        journal = self.journal
        event_id = self.events[0]['event_id']
        for action in JournalUpdateAction:
            update = self.JournalUpdate.make(self.journal.id_, event_id, action)
            self.assertEqual(update.journal_id, journal.id_)
            self.assertEqual(update.event_id, event_id)
            self.assertEqual(update.action, action)

    def test_list_journal_updates(self):
        class JournalUpdate(BaseJournalUpdate):
            bucket = self.bucket
            _pfx = f"{self.root_pfx}/test_list_updates"

        living_updates, _ = self._generate_and_upload_test_updates(JournalUpdate._pfx)

        with self.subTest("Listing should not include tombstoned updates"):
            self.assertEqual(living_updates, [id_ for id_ in JournalUpdate.list()])

        with self.subTest("Listing of out of date journals should include only journal with non-tombstoned updates"):
            self.assertEqual(set(uid.journal_id for uid in living_updates),
                             set(jid for jid in JournalUpdate.list_out_of_date_journals()))

        with self.subTest("Updates for journals should not include tombstones, and should have correct structure"):
            expected_updates = defaultdict(dict)
            for uid in living_updates:
                expected_updates[uid.journal_id][uid.event_id] = JournalUpdate(uid)
            for journal_id, updates in JournalUpdate.get_updates_for_all_journals():
                self.assertEqual(updates.keys(), expected_updates[journal_id].keys())
                self.assertEqual(JournalUpdate.get_updates_for_journal(journal_id).keys(),
                                 expected_updates[journal_id].keys())
                del expected_updates[journal_id]
            self.assertEqual(0, len(expected_updates))

    def _generate_and_upload_test_updates(self, pfx: str, number_of_updates: int=50):
        living_updates = list()
        dead_updates = list()
        journal_id = _random_journal_id()
        for _ in range(40):
            action = list(JournalUpdateAction)[randint(0, len(JournalUpdateAction) - 1)]
            if not randint(0, 3):
                journal_id = _random_journal_id()
            journal_update_id = JournalUpdateID.make(journal_id, str(uuid4()), action)
            self.bucket.Object(key=f"{pfx}/{journal_update_id}").upload_fileobj(io.BytesIO(b""))
            if not randint(0, 2):
                self.bucket.Object(key=f"{pfx}/{journal_update_id}{TOMBSTONE_SUFFIX}").upload_fileobj(io.BytesIO(b""))
                dead_updates.append(journal_update_id)
            else:
                living_updates.append(journal_update_id)
        return sorted(living_updates), sorted(dead_updates)


def _random_journal_id(version: str="test_version", blob_id: str="test_blob_id"):
    start_timestamp = random_date()
    return JournalID.make(datetime_to_timestamp(start_timestamp),
                          datetime_to_timestamp(start_timestamp + timedelta(days=1)),
                          version,
                          blob_id)

if __name__ == '__main__':
    unittest.main()
