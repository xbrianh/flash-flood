#!/usr/bin/env python
import os
import sys
from uuid import uuid4
import unittest
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import randint

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import flashflood
from flashflood.util import datetime_from_timestamp


class TestFlashFlood(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root_pfx = f"flashflood_test_{uuid4()}"
        cls.bucket = boto3.resource("s3").Bucket(os.environ['FLASHFLOOD_TEST_BUCKET'])
        cls.flashflood = flashflood.FlashFlood(cls.bucket.name, cls.root_pfx)

    @classmethod
    def tearDownClass(cls):
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(item.delete)
                       for item in cls.bucket.objects.filter(Prefix="flashflood_test_")]
            for f in as_completed(futures):
                f.result()

    def tearDown(self):
        self.flashflood._delete_all_journals()

    def test_get_event(self):
        events = self.generate_events(10, journal=False)
        with self.subTest("Get event before journaling"):
            for _ in range(3):
                event_id = [event_id for event_id in events][randint(0, 9)]
                event = self.flashflood.get_event(event_id)
                self.assertEqual(event.data, events[event_id].data)
        self.flashflood.journal(10)
        with self.subTest("Get event after journaling"):
            for _ in range(3):
                event_id = [event_id for event_id in events][randint(0, 9)]
                event = self.flashflood.get_event(event_id)
                self.assertEqual(event.data, events[event_id].data)
        with self.subTest("Get non-existent event"):
            with self.assertRaises(flashflood.FlashFloodEventNotFound):
                self.flashflood.get_event("no_such_event")

    def test_update_event(self):
        events = self.generate_events(10, journal=False)
        with self.subTest("Update event before journaling"):
            for _ in range(3):
                event_id = [event_id for event_id in events][randint(0, 9)]
                new_data = os.urandom(5)
                self.flashflood.update_event(new_data, event_id)
                event = self.flashflood.get_event(event_id)
                self.assertEqual(event.data, new_data)
                events[event.event_id] = event
        self.flashflood.journal(10)
        with self.subTest("Update event after journaling"):
            for _ in range(3):
                event_id = [event_id for event_id in events][randint(0, 9)]
                new_data = os.urandom(5)
                self.flashflood.update_event(new_data, event_id)
                event = self.flashflood.get_event(event_id)
                self.assertEqual(event.data, new_data)
                events[event.event_id] = event
        with self.subTest("Replay after event update"):
            for event in self.flashflood.replay():
                self.assertEqual(event.data, events[event.event_id].data)

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
            self.flashflood.journal(events_per_journal)
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
            while True:
                replay_urls = self.flashflood.replay_urls(from_date, to_date, maximum_number_of_results=1)
                if not replay_urls:
                    break
                new_retrieved_events = [event for event in flashflood.replay_with_urls(replay_urls, from_date, to_date)]
                for event in new_retrieved_events:
                    self.assertGreater(event.date, from_date)
                retrieved_events.extend(new_retrieved_events)
                from_date = datetime_from_timestamp(replay_urls[-1]['manifest']['to_date'])
                if not (from_date < to_date):
                    break
            self.assertEqual(len(dates) - 2, len(retrieved_events))

    # TODO: and DateRange tests

    def test_url_range(self):
        """
        Partial date requests should download only a range of the journal
        """
        events = self.generate_events(10)
        events = sorted([e for e in events.values()], key=lambda e: e.date)
        from_date = events[3].date
        replay_urls = self.flashflood.replay_urls(from_date)
        retrieved_events = [event for event in flashflood.replay_with_urls(replay_urls, from_date)]
        for event in events[:4]:
            self.assertNotIn(event, retrieved_events)
        for event in events[4:]:
            self.assertIn(event, retrieved_events)

    def test_journal(self):
        self.generate_events(1, journal=False)
        with self.assertRaises(flashflood.FlashFloodCollationError):
            self.flashflood.journal(number_of_events=2)

    def test_urls(self):
        events = dict()
        events.update(self.generate_events())
        events.update(self.generate_events())
        replay_urls = self.flashflood.replay_urls(maximum_number_of_results=2)
        retrieved_events = {event.event_id: event
                            for event in flashflood.replay_with_urls(replay_urls)}
        for event_id in events:
            self.assertEqual(events[event_id].data, retrieved_events[event_id].data)

    def test_get_new_journals(self):
        events = self.generate_events(3, journal=False)
        new_journals = [c for c in self.flashflood._get_new_journals(len(events) - 1)]
        self.assertEqual(len(events) - 1, len(new_journals))

    def generate_events(self, number_of_events=7, journal=True):
        def _put():
            event_id = str(uuid4()) + ".asdj__argh"
            return self.flashflood.put(os.urandom(3), event_id, self._random_timestamp())

        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(_put) for _ in range(number_of_events)]
            events = {f.result().event_id: f.result() for f in futures}
        if journal:
            self.flashflood.journal(number_of_events=number_of_events)
        return events

    def _random_timestamp(self):
        year = "%04i" % randint(1000, 2019)
        month = "%02i" % randint(1, 12)
        day = "%02i" % randint(1, 28)
        hours = "%02i" % randint(0, 23)
        minutes = "%02i" % randint(0, 59)
        seconds = "%02i" % randint(0, 59)
        fractions = "%06i" % randint(1, 999999)
        timestamp = f"{year}-{month}-{day}T{hours}{minutes}{seconds}.{fractions}Z"
        return datetime_from_timestamp(timestamp)

if __name__ == '__main__':
    unittest.main()
