#!/usr/bin/env python
import io
import os
import sys
from uuid import uuid4
import unittest
import boto3
import json
from tempfile import gettempdir

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from flashflood import flashflood
from tests import infra


class TestFlashFlood(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root_pfx = f"flashflood_test_{uuid4()}"
        cls.bucket = boto3.resource("s3").Bucket(os.environ['FLASHFLOOD_TEST_BUCKET'])
        cls.flashflood = flashflood.FlashFlood(cls.bucket.name, cls.root_pfx)

    @classmethod
    def tearDownClass(cls):
        cls.flashflood._delete_all()

    def test_events(self):
        events = dict()
        events.update(self.generate_events())
        events.update(self.generate_events(5, collate=False))
        retrieved_events = {event.uid: event for event in self.flashflood.events()}
        for event_id in events:
            self.assertEqual(events[event_id].data, retrieved_events[event_id].data)

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

if __name__ == '__main__':
    unittest.main()
