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

from flashflood import flashflood
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

    def test_events(self):
        events = dict()
        events.update(self.generate_events())
        events.update(self.generate_events(5, collate=False))
        retrieved_events = {event.uid: event for event in self.flashflood.events()}
        for event_id in events:
            self.assertEqual(events[event_id].data, retrieved_events[event_id].data)

    def test_date_sorts(self, number_of_collations=3, items_per_collation=3):
        self.flashflood._delete_all()
        timestamps = [self._random_timestamp() for _ in range(number_of_collations * items_per_collation)]
        with ThreadPoolExecutor(max_workers=10) as e:
            futures = [e.submit(self.flashflood.put, os.urandom(5), timestamp=ts)
                       for ts in timestamps]
            events = {event.uid: event for event in [f.result() for f in futures]}
        for _ in range(number_of_collations):
            self.flashflood.collate(number_of_events=items_per_collation)
        from_date = datetime_from_timestamp(sorted(timestamps)[1])
        retrieved_events = [event for event in self.flashflood.events(from_date=from_date)]
        for event in retrieved_events:
            self.assertGreaterEqual(datetime_from_timestamp(event.timestamp), from_date)
        self.assertEqual(len(timestamps) - 1, len(retrieved_events))

    def test_collation(self):
        self.generate_events(1, collate=False)
        with self.assertRaises(flashflood.FlashFloodCollationError):
            self.flashflood.collate(number_of_events=2)

    def test_urls(self):
        events = dict()
        events.update(self.generate_events())
        events.update(self.generate_events())
        event_urls = self.flashflood.event_urls()
        retrieved_events = {event.uid: event
                            for event in flashflood.events_for_presigned_urls(event_urls)}
        for event_id in events:
            self.assertEqual(events[event_id].data, retrieved_events[event_id].data)

    def generate_events(self, number_of_events=7, collate=True):
        events = dict()
        for _ in range(number_of_events):
            event_id = str(uuid4()) + ".asdj__argh"
            events[event_id] = self.flashflood.put(os.urandom(10), event_id)
        if collate:
            self.flashflood.collate(number_of_events=number_of_events)
        return events

    def _random_timestamp(self):
        year = "%04i" % randint(1,2019)
        month = "%02i" % randint(1, 12)
        day = "%02i" % randint(1, 28)
        hours = "%02i" % randint(1, 23)
        minutes = "%02i" % randint(1, 59)
        seconds = "%02i" % randint(1, 59)
        fractions = "%06i" % randint(1, 999999)
        return f"{year}-{month}-{day}T{hours}{minutes}{seconds}.{fractions}Z"

if __name__ == '__main__':
    unittest.main()
