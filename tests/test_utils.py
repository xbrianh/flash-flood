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

from flashflood import config
from flashflood.util import concurrent_listing, delete_keys, S3Deleter, upload_object, update_object_tagging
from tests import infra


class TestUtils(unittest.TestCase):
    bucket: typing.Any = None
    s3_client: typing.Any = None
    root_pfx: typing.Optional[str] = None

    @classmethod
    def setUpClass(cls):
        cls.root_pfx = f"flashflood-test-utils-{uuid4()}"
        cls.s3 = boto3.resource("s3")
        cls.s3_client = cls.s3.meta.client
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

    def test_upload_object(self):
        with self.subTest("Test upload without verification"):
            self._test_upload_object(False)
        with self.subTest("Test upload with verification"):
            self._test_upload_object(True)

    def _test_upload_object(self, verify: bool):
        config.object_exists_waiter_config['Delay'] = int(verify)
        tagging = dict(foo="bar", doom="gloom")
        metadata = dict(sed="awk", perl="ruby")
        key = f"{self.root_pfx}/{uuid4()}"
        data = os.urandom(2)
        verified = upload_object(self.s3_client, self.bucket.name, key, data, tagging, metadata)
        obj = self.bucket.Object(key)
        self.assertEqual(verify, verified)
        self.assertEqual(obj.metadata, metadata)
        self.assertEqual(obj.get()['Body'].read(), data)
        self.assertEqual(tagging, self._get_tagging(key))

    def test_update_object_tagging(self):
        key = self._upload_objects(1)[0]
        tagging = dict(foo="bar")
        update_object_tagging(self.s3_client, self.bucket.name, key, tagging)
        self.assertEqual(tagging, self._get_tagging(key))

    def _get_tagging(self, key):
        tagset = self.s3_client.get_object_tagging(Bucket=self.bucket.name, Key=key)['TagSet']
        return {tag['Key']: tag['Value'] for tag in tagset}

    def _upload_objects(self, number_of_objects: int=10) -> typing.List[str]:
        keys = [f"{self.root_pfx}/{uuid4()}" for _ in range(number_of_objects)]
        for key in keys:
            self.bucket.Object(key).upload_fileobj(io.BytesIO(os.urandom(1)))
        return keys

if __name__ == '__main__':
    unittest.main()
