import io
import typing

from flashflood.util import delete_keys

class BaseKeyIndex:
    """
    Build a simple key value index using s3 keys. Updates are modeled as writes to avoid S3 eventual consistency for
    overwrites.

    Concurrent writes are not supported.
    """
    DELIMITER: str = "--"
    bucket: typing.Any = None
    _pfx: typing.Optional[str] = None

    @classmethod
    def put(cls, lookup: str, target: str):
        keys = cls._put(lookup, target)
        delete_keys(cls.bucket, keys)

    @classmethod
    def put_batch(cls, lookup_map: dict):
        keys_to_delete = list()
        for lookup, target in lookup_map.items():
            keys = cls._put(lookup, target)
            keys_to_delete.extend(keys)
        delete_keys(cls.bucket, keys_to_delete)

    @classmethod
    def _put(cls, lookup: str, target: str) -> list:
        keys = cls._lookup_keys(lookup)
        if keys:
            revision_number = cls._revision_number_for_key(keys[-1]) + 1
        else:
            revision_number = 1
        revision = "%010i" % revision_number
        key = f"{cls._pfx}/{lookup}" + cls.DELIMITER + revision
        cls.bucket.Object(key).upload_fileobj(io.BytesIO(b""),
                                              ExtraArgs=dict(Metadata=dict(target=target)))
        return keys

    @classmethod
    def delete(cls, lookup: str):
        keys = cls._lookup_keys(lookup)
        if keys:
            delete_keys(cls.bucket, keys)

    @classmethod
    def get(cls, lookup: str):
        keys = cls._lookup_keys(lookup)
        if keys:
            return cls.bucket.Object(keys[-1]).metadata['target']
        else:
            return None

    @classmethod
    def _lookup_keys(cls, lookup: str):
        return [item.key for item in cls.bucket.objects.filter(Prefix=f"{cls._pfx}/{lookup}")]

    @classmethod
    def _revision_number_for_key(cls, key: str) -> int:
        return int(key.rsplit(cls.DELIMITER, 1)[1])
