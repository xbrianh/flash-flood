#!/usr/bin/env python
import io
import os
import sys
import time
import typing
from uuid import uuid4
import unittest
from string import hexdigits

import boto3

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from flashflood.util import concurrent_listing, delete_keys, S3Deleter
from tests import infra


class TestUtils(unittest.TestCase):
    bucket: typing.Any = None
    root_pfx: typing.Optional[str] = None

    @classmethod
    def setUpClass(cls):
        cls.root_pfx = f"flashflood-test-utils-{uuid4()}"
        cls.s3 = boto3.resource("s3")
        cls.bucket = cls.s3.Bucket(infra.get_env("S3_BUCKET"))

    @classmethod
    def tearDownClass(cls):
        keys_to_delete = [item.key for item in concurrent_listing(cls.bucket, [f"{cls.root_pfx}/"])]
        delete_keys(cls.bucket, keys_to_delete)

    # TODO: Add DateRange tests

    def test_concurrent_listing(self):
        keys = self._upload_objects()
        prefixes = [f"{self.root_pfx}/{c}" for c in hexdigits.lower()]
        listed_keys = [item.key for item in concurrent_listing(self.bucket, prefixes)]
        self.assertEqual(set(keys), set(listed_keys))
        with self.assertRaises(AssertionError):
            next(concurrent_listing(self.bucket, "alskdjf"))

    def test_delete_keys(self):
        self._upload_objects()
        keys_to_delete = {item.key for item in self.bucket.objects.filter(Prefix=f"{self.root_pfx}/")}
        delete_keys(self.bucket, list(keys_to_delete))
        self._assert_keys_not_listed(f"{self.root_pfx}/", keys_to_delete)

    def test_s3_deleter(self):
        self._upload_objects()
        keys_to_delete = {item.key for item in self.bucket.objects.filter(Prefix=f"{self.root_pfx}/")}
        with S3Deleter(self.bucket) as s3d:
            for key in list(keys_to_delete):
                s3d.delete(key)
        self._assert_keys_not_listed(f"{self.root_pfx}/", keys_to_delete)

    def _assert_keys_not_listed(self, pfx: str, keys: set):
        for _ in range(10):
            listed_keys = {item.key for item in self.bucket.objects.filter(Prefix=f"{pfx}/")}
            if listed_keys.intersection(keys):
                time.sleep(6)
        self.assertEqual(0, len(listed_keys.intersection(keys)))

    def _upload_objects(self, number_of_objects: int=10) -> typing.List[str]:
        keys = [f"{self.root_pfx}/{uuid4()}" for _ in range(number_of_objects)]
        for key in keys:
            self.bucket.Object(key).upload_fileobj(io.BytesIO(os.urandom(1)))
        return keys

if __name__ == '__main__':
    unittest.main()
