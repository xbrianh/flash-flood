#!/usr/bin/env python
import os
import sys
from uuid import uuid4
import unittest
import boto3
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import randint

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import flashflood
from flashflood.util import datetime_from_timestamp, delete_keys
from flashflood.exceptions import FlashFloodException, FlashFloodEventNotFound, FlashFloodEventExistsError
from tests import infra, random_date


class TestFlashFlood(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.s3 = boto3.resource("s3")
        cls.bucket = cls.s3.Bucket(infra.get_env("S3_BUCKET"))

    @classmethod
    def tearDownClass(cls):
        keys = [item.key for item in cls.bucket.objects.filter(Prefix=f"flashflood_test_")]
        delete_keys(cls.bucket, keys)

    def setUp(self):
        self.root_pfx = f"flashflood_test_{uuid4()}"
        self.flashflood = flashflood.FlashFlood(self.s3, self.bucket.name, self.root_pfx)

    def tearDown(self):
        self.flashflood._destroy()

    def test_put_and_update_event_exceptions(self):
        events = self.generate_events(1, journal=False)
        with self.subTest("Should not be able to put event_id with illegal chars"):
            with self.assertRaises(FlashFloodException):
                self.flashflood.put(b"", "asldkfj--lasjf")
        with self.subTest("Should not be able to put an existing event"):
            with self.assertRaises(FlashFloodEventExistsError):
                self.flashflood.put(b"", next(iter(events.values())).event_id)
        with self.subTest("Should not be able to update a non-existent event"):
            with self.assertRaises(FlashFloodEventNotFound):
                self.flashflood.update_event(str(uuid4()), b"")

    def test_get_event(self):
        events = self.generate_events(10, journal=False)
        with self.subTest("Get event before journaling"):
            for _ in range(3):
                event_id = [event_id for event_id in events][randint(0, 9)]
                self.assertTrue(self.flashflood.event_exists(event_id))
                event = self.flashflood.get_event(event_id)
                self.assertEqual(event.data, events[event_id].data)
        self.flashflood.journal(minimum_number_of_events=10)
        with self.subTest("Get event after journaling"):
            for _ in range(3):
                event_id = [event_id for event_id in events][randint(0, 9)]
                self.assertTrue(self.flashflood.event_exists(event_id))
                event = self.flashflood.get_event(event_id)
                self.assertEqual(event.data, events[event_id].data)
        with self.subTest("Get non-existent event"):
            self.assertFalse(self.flashflood.event_exists("no_such_event"))
            with self.assertRaises(flashflood.FlashFloodEventNotFound):
                self.flashflood.get_event("no_such_event")

    def test_updates_and_deletes_2(self):
        number_of_events = 2
        with self.subTest("Test update event"):
            events = self.generate_events(number_of_events, journal=True)
            event_id = set(events.keys()).pop()
            new_event_data = self._random_data()
            self.flashflood.update_event(event_id, new_event_data)
            self._find_event_test(event_id, events[event_id].data)
            self.flashflood.update()
            self._find_event_test(event_id, new_event_data)
        self.assertEqual(0, len([j for j in self.flashflood._new_journals()]))
        with self.subTest("Test update new event"):
            events = self.generate_events(number_of_events, journal=False)
            event_id = set(events.keys()).pop()
            new_event_data = self._random_data()
            self.flashflood.update_event(event_id, new_event_data)
            self._find_event_test(event_id, events[event_id].data)
            self.flashflood.journal(minimum_number_of_events=number_of_events)
            self._find_event_test(event_id, new_event_data)
        self.assertEqual(0, len([j for j in self.flashflood._new_journals()]))
        with self.subTest("Test delete event"):
            events = self.generate_events(number_of_events, journal=True)
            event_id = set(events.keys()).pop()
            self.flashflood.delete_event(event_id)
            self._find_event_test(event_id, should_find_event_in_replay=True, should_find_event_in_lookup=False)
            self.flashflood.update()
            self._find_event_test(event_id, should_find_event_in_replay=False, should_find_event_in_lookup=False)
        self.assertEqual(0, len([j for j in self.flashflood._new_journals()]))
        with self.subTest("Test delete new event"):
            events = self.generate_events(number_of_events, journal=False)
            event_id = set(events.keys()).pop()
            self.flashflood.delete_event(event_id)
            self._find_event_test(event_id, should_find_event_in_replay=True, should_find_event_in_lookup=False)
            self.flashflood.journal(minimum_number_of_events=number_of_events)
            self._find_event_test(event_id, should_find_event_in_replay=False, should_find_event_in_lookup=False)

    def _find_event_test(self,
                         event_id: str,
                         expected_data: bytes=None,
                         should_find_event_in_replay: bool=True,
                         should_find_event_in_lookup: bool=True):
        event_found = False
        for event in self.flashflood.replay():
            if event.event_id == event_id:
                event_found = True
                break
        self.assertTrue(event_found == should_find_event_in_replay)
        if expected_data is not None:
            self.assertEqual(event.data, expected_data)

        if should_find_event_in_lookup:
            event = self.flashflood.get_event(event_id)
            if expected_data is not None:
                self.assertEqual(event.data, expected_data)
        else:
            with self.assertRaises(FlashFloodEventNotFound):
                self.flashflood.get_event(event_id)

    def test_updates_and_deletes_1(self):
        events = self.generate_events(15, journal=False)
        with self.subTest("Update event before journaling"):
            self._test_update(events)
        with self.subTest("Delete event before journaling"):
            self._test_delete(events)
        with self.subTest("Update event after journaling"):
            self._test_update(events, number_of_events_to_journal=4)
        with self.subTest("Delete event after journaling"):
            self._test_delete(events, number_of_events_to_journal=4)
        with self.subTest("Replay after event update"):
            self._test_replay(expected_events=events)

    def _test_replay(self, expected_events, number_of_retries=5):
        """
        Remove replayed events from `expected_events` until empty. Fail otherwise.
        Retry to avoid failing due to s3 eventual consistency.
        """
        expected_events = expected_events.copy()
        for _ in range(number_of_retries):
            for event in self.flashflood.replay():
                if event.event_id in expected_events:
                    if event.data == expected_events[event.event_id].data:
                        del expected_events[event.event_id]
            if not expected_events:
                return
            time.sleep(1.0)
        self.fail("Expected events did not appear correctly in event replay")

    def _test_update(self, events, number_of_updates=2, number_of_events_to_journal=0):
        new_event_data = dict()
        for _ in range(number_of_updates):
            event_id = [event_id for event_id in events][randint(0, len(events) - 1)]
            new_event_data[event_id] = self._random_data()
            self.flashflood.update_event(event_id, new_event_data[event_id])
        if 0 < number_of_events_to_journal:
            self.flashflood.journal(minimum_number_of_events=number_of_events_to_journal // 2)
            self.flashflood.update()
            self.flashflood.journal(minimum_number_of_events=number_of_events_to_journal // 2)
        else:
            self.flashflood.update()
        for event_id in new_event_data:
            event = self.flashflood.get_event(event_id)
            self.assertEqual(event.data, new_event_data[event_id])
            events[event.event_id] = event

    def _test_delete(self, events, number_of_deletes=2, number_of_events_to_journal=0):
        deleted_events = list()
        for _ in range(number_of_deletes):
            event_id = [event_id for event_id in events][randint(0, len(events) - 1)]
            deleted_events.append(event_id)
            del events[event_id]
            self.flashflood.delete_event(event_id)
        if 0 < number_of_events_to_journal:
            self.flashflood.journal(minimum_number_of_events=number_of_events_to_journal // 2)
            self.flashflood.update()
            self.flashflood.journal(minimum_number_of_events=number_of_events_to_journal // 2)
        else:
            self.flashflood.update()

    def test_events(self):
        events = dict()
        events.update(self.generate_events())
        events.update(self.generate_events(5, journal=False))
        retrieved_events = {event.event_id: event for event in self.flashflood.replay()}
        for event_id in events:
            self.assertEqual(events[event_id].data, retrieved_events[event_id].data)

    def test_ordering(self, number_of_journals=3, events_per_journal=3):
        events = self.generate_events(number_of_journals * events_per_journal, journal=False)
        for _ in range(number_of_journals):
            self.flashflood.journal(minimum_number_of_events=events_per_journal)
        dates = [e.date for e in events.values()]

        with self.subTest("events should be returned in order with date > from_date"):
            ordered_dates = sorted(dates)
            from_date = ordered_dates[0]
            to_date = ordered_dates[-2]
            retrieved_events = [event for event in self.flashflood.replay(from_date, to_date)]
            for event in retrieved_events:
                self.assertGreater(event.date, from_date)
                self.assertLessEqual(event.date, to_date)
            self.assertEqual(len(dates) - 2, len(retrieved_events))

        with self.subTest("events via urls should be returned in order with date > from_date"):
            ordered_dates = sorted(dates)
            from_date = ordered_dates[0]
            to_date = ordered_dates[-2]
            retrieved_events = list()
            for manifest in self.flashflood.list_event_streams(from_date, to_date):
                new_retrieved_events = list()
                for event in flashflood.replay_event_stream(manifest, from_date, to_date):
                    self.assertGreater(event.date, from_date)
                    new_retrieved_events.append(event)
                retrieved_events.extend(new_retrieved_events)
                from_date = datetime_from_timestamp(manifest['to_date'])
                if not (from_date < to_date):
                    break
            self.assertEqual(len(dates) - 2, len(retrieved_events))

    def test_url_range(self):
        """
        Partial date requests should download only a range of the journal
        """
        events = self.generate_events(10)
        events = sorted([e for e in events.values()], key=lambda e: e.date)
        from_date = events[3].date
        retrieved_events = [event
                            for manifest in self.flashflood.list_event_streams(from_date)
                            for event in flashflood.replay_event_stream(manifest, from_date)]
        for event in events[:4]:
            self.assertNotIn(event, retrieved_events)
        for event in events[4:]:
            self.assertIn(event, retrieved_events)

    def test_journal(self):
        self.generate_events(1, journal=False)
        with self.subTest("raise FlashFloodJournalingError when attempting to journal more new events than available"):
            with self.assertRaises(flashflood.FlashFloodJournalingError):
                self.flashflood.journal(minimum_number_of_events=2)
        with self.subTest("raise FlashFloodJournalingError when new event data doesn't meet size threshold"):
            with self.assertRaises(flashflood.FlashFloodJournalingError):
                self.flashflood.journal(minimum_size=10)
        self.generate_events(4, journal=False)
        with self.subTest("Should succeed when minimum number and size thresholds are met"):
            self.flashflood.journal(minimum_number_of_events=5, minimum_size=5)

    def test_event_streams(self):
        events = dict()
        events.update(self.generate_events())
        events.update(self.generate_events())
        retrieved_events = {event.event_id: event
                            for event_stream in self.flashflood.list_event_streams()
                            for event in flashflood.replay_event_stream(event_stream)}
        for event_id in events:
            self.assertEqual(events[event_id].data, retrieved_events[event_id].data)

    def test_get_new_journals(self):
        number_of_events = 3
        self.generate_events(number_of_events, journal=False)
        new_journals = [j for j in self.flashflood._new_journals()]
        self.assertEqual(number_of_events, len(new_journals))

    def generate_events(self, number_of_events=7, journal=True):
        def _put():
            event_id = str(uuid4()) + ".asdj__argh"
            return self.flashflood.put(self._random_data(), event_id, random_date())

        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(_put) for _ in range(number_of_events)]
            events = {f.result().event_id: f.result() for f in futures}
        if journal:
            self.flashflood.journal(minimum_number_of_events=number_of_events)
        return events

    def _random_data(self, lower_size_limit=1, upper_size_limit=10) -> bytes:
        return os.urandom(randint(lower_size_limit, upper_size_limit))

if __name__ == '__main__':
    unittest.main()
