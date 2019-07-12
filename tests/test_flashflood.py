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
from flashflood.util import datetime_from_timestamp, distant_past


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
        self.flashflood._delete_all_collations()

    def test_events(self):
        events = dict()
        events.update(self.generate_events())
        events.update(self.generate_events(5, collate=False))
        retrieved_events = {event.uid: event for event in self.flashflood.events()}
        for event_id in events:
            self.assertEqual(events[event_id].data, retrieved_events[event_id].data)

    def test_ordering(self, number_of_collations=3, items_per_collation=3):
        events = self.generate_events(number_of_collations * items_per_collation, collate=False)
        for _ in range(number_of_collations):
            self.flashflood.collate(items_per_collation)
        dates = [e.date for e in events.values()]

        with self.subTest("events should be returned in order with date > from_date"):
            from_date = sorted(dates)[0]
            retrieved_events = [event for event in self.flashflood.events(from_date=from_date)]
            for event in retrieved_events:
                self.assertGreater(event.date, from_date)
            self.assertEqual(len(dates) - 1, len(retrieved_events))

        with self.subTest("events via urls should be returned in order with date > from_date"):
            from_date = distant_past
            while True:
                event_urls = self.flashflood.event_urls(from_date, 1)
                if not event_urls:
                    break
                retrieved_events = [event for event in flashflood.events_from_urls(event_urls, from_date)]
                for event in retrieved_events:
                    self.assertGreater(event.date, from_date)
                    from_date = event.date

    def test_url_range(self):
        """
        Partial date requests should download only a range of the collation
        """
        events = self.generate_events(10)
        events = sorted([e for e in events.values()], key=lambda e: e.date)
        from_date = events[3].date
        event_urls = self.flashflood.event_urls(from_date)
        retrieved_events = [event for event in flashflood.events_from_urls(event_urls, from_date)]
        for event in events[:4]:
            self.assertNotIn(event, retrieved_events)
        for event in events[4:]:
            self.assertIn(event, retrieved_events)

    def test_collation(self):
        self.generate_events(1, collate=False)
        with self.assertRaises(flashflood.FlashFloodCollationError):
            self.flashflood.collate(number_of_events=2)

    def test_urls(self):
        events = dict()
        events.update(self.generate_events())
        events.update(self.generate_events())
        event_urls = self.flashflood.event_urls(number_of_pages=2)
        retrieved_events = {event.uid: event
                            for event in flashflood.events_from_urls(event_urls)}
        for event_id in events:
            self.assertEqual(events[event_id].data, retrieved_events[event_id].data)

    def test_get_new_collations(self):
        events = self.generate_events(3, collate=False)
        new_collations = [c for c in self.flashflood._get_new_collations(len(events) - 1)]
        self.assertEqual(len(events) - 1, len(new_collations))

    def generate_events(self, number_of_events=7, collate=True):
        def _put():
            event_id = str(uuid4()) + ".asdj__argh"
            return self.flashflood.put(os.urandom(3), event_id, self._random_timestamp())

        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(_put) for _ in range(number_of_events)]
            events = {f.result().uid: f.result() for f in futures}
        if collate:
            self.flashflood.collate(number_of_events=number_of_events)
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
