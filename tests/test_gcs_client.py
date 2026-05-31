"""Tests for the GCS helper module (services/gcs.py).

Covers URI parsing, upload/download/delete adapters around a fake client,
and listing prefixes.
"""
from __future__ import annotations

import pytest

from cron_job.services import gcs


# ---------------------------------------------------------------------------
# Fake storage client
# ---------------------------------------------------------------------------


class FakeBlob:
    def __init__(self, name: str, exists: bool = False, data: bytes = b""):
        self.name = name
        self._exists = exists
        self._data = data
        self.uploaded: list[tuple[bytes, str]] = []
        self.deleted = False

    def upload_from_string(self, data, content_type=None):
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._data = data
        self._exists = True
        self.uploaded.append((data, content_type))

    def download_as_bytes(self):
        return self._data

    def delete(self):
        self.deleted = True
        self._exists = False

    def exists(self):
        return self._exists


class FakeBucket:
    def __init__(self, name: str):
        self.name = name
        self.blobs: dict[str, FakeBlob] = {}

    def blob(self, blob_name: str) -> FakeBlob:
        return self.blobs.setdefault(blob_name, FakeBlob(blob_name))

    def list_blobs(self, prefix: str):
        return [
            blob
            for name, blob in self.blobs.items()
            if name.startswith(prefix)
        ]


class FakeStorageClient:
    def __init__(self):
        self.buckets: dict[str, FakeBucket] = {}

    def bucket(self, name: str) -> FakeBucket:
        return self.buckets.setdefault(name, FakeBucket(name))


# ---------------------------------------------------------------------------
# parse_gcs_uri / make_gcs_uri
# ---------------------------------------------------------------------------


def test_parse_gcs_uri_returns_bucket_and_blob():
    bucket, blob = gcs.parse_gcs_uri("gs://my-bucket/path/to/object.csv")
    assert bucket == "my-bucket"
    assert blob == "path/to/object.csv"


def test_parse_gcs_uri_rejects_non_gs_scheme():
    with pytest.raises(ValueError):
        gcs.parse_gcs_uri("https://example.com/foo")


def test_parse_gcs_uri_rejects_bucket_only_uri():
    with pytest.raises(ValueError):
        gcs.parse_gcs_uri("gs://just-bucket")


def test_make_gcs_uri_strips_leading_slash_on_blob():
    assert gcs.make_gcs_uri("bucket", "/foo/bar.csv") == "gs://bucket/foo/bar.csv"
    assert gcs.make_gcs_uri("bucket", "foo/bar.csv") == "gs://bucket/foo/bar.csv"


# ---------------------------------------------------------------------------
# upload / download / delete
# ---------------------------------------------------------------------------


def test_upload_text_writes_utf8_bytes_with_content_type():
    client = FakeStorageClient()
    gcs.upload_text(client, "gs://b/data.csv", "héllo,wörld", content_type="text/csv")

    blob = client.bucket("b").blob("data.csv")
    assert blob.uploaded == [("héllo,wörld".encode("utf-8"), "text/csv")]


def test_download_text_round_trips_utf8_payload():
    client = FakeStorageClient()
    gcs.upload_text(client, "gs://b/file.txt", "round-trip ✓")
    assert gcs.download_text(client, "gs://b/file.txt") == "round-trip ✓"


def test_blob_exists_returns_true_only_after_upload():
    client = FakeStorageClient()
    assert gcs.blob_exists(client, "gs://b/missing.txt") is False
    gcs.upload_text(client, "gs://b/missing.txt", "x")
    assert gcs.blob_exists(client, "gs://b/missing.txt") is True


def test_delete_blob_clears_existence():
    client = FakeStorageClient()
    gcs.upload_text(client, "gs://b/x.txt", "data")
    gcs.delete_blob(client, "gs://b/x.txt")
    assert gcs.blob_exists(client, "gs://b/x.txt") is False


def test_safe_delete_many_swallows_exceptions():
    class ErroringClient:
        def bucket(self, _name):
            class Bucket:
                def blob(self, _bn):
                    class Blob:
                        def delete(self):
                            raise RuntimeError("boom")

                    return Blob()

            return Bucket()

    # Must not raise
    gcs.safe_delete_many(ErroringClient(), ["gs://b/a.txt", "gs://b/b.txt"])


def test_list_blob_uris_returns_full_gs_uris_filtered_by_prefix():
    client = FakeStorageClient()
    gcs.upload_text(client, "gs://b/a/one.csv", "1")
    gcs.upload_text(client, "gs://b/a/two.csv", "2")
    gcs.upload_text(client, "gs://b/other/three.csv", "3")

    uris = gcs.list_blob_uris(client, "b", "a/")
    assert sorted(uris) == ["gs://b/a/one.csv", "gs://b/a/two.csv"]
