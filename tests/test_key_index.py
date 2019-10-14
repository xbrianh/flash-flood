#!/usr/bin/env python
import io
import os
import sys
import time
import typing
from uuid import uuid4
import unittest

import boto3

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from flashflood.key_index import BaseKeyIndex
from flashflood.util import concurrent_listing, delete_keys
from tests import infra


class TestKeyIndex(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root_pfx = f"flashflood-test-key-index-{uuid4()}"
        cls.s3 = boto3.resource("s3")
        cls.bucket = cls.s3.Bucket(infra.get_env("S3_BUCKET"))

    @classmethod
    def tearDownClass(cls):
        keys_to_delete = [item.key for item in concurrent_listing(cls.bucket, [f"{cls.root_pfx}/"])]
        delete_keys(cls.bucket, keys_to_delete)

    def setUp(self):
        class KeyIndex(BaseKeyIndex):
            bucket = self.bucket
            _index_pfx = self.root_pfx

        self.index = KeyIndex

    def test_key_index_put(self):
        self.index.put("foo", "bar")
        self.assertEqual(self.index.get("foo"), "bar")

    def test_key_index_put_batch(self):
        items = {str(uuid4()): str(uuid4()) for _ in range(10)}
        self.index.put_batch(items)
        for key, val in items.items():
            self.assertEqual(self.index.get(key), val)


if __name__ == '__main__':
    unittest.main()
