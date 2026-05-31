"""Tests for services/firestore/writeback.py.

Covers:
- _truthy parsing of "selected" / boolean-like strings
- read_selected_image_rows decoding shard CSVs and using fallback meta_image_uuid
- build_selected_image_prefixes filtering and bucket fallback
- write_back_preprocessing_status: status calculation + 400-row batch chunking
- Failure swallowing on Firestore commit errors
"""
from __future__ import annotations

import csv
import io

import pytest

from cron_job.services.firestore.writeback import (
    _batch_set,
    _truthy,
    build_selected_image_prefixes,
    read_selected_image_rows,
    write_back_preprocessing_status,
)

from .conftest import CaptureLogger, make_image_doc


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeBatch:
    def __init__(self, fail: bool = False):
        self.writes: list[tuple[str, dict]] = []
        self._fail = fail
        self.committed = False

    def set(self, ref, payload, merge=True):
        self.writes.append((str(ref), payload))

    def commit(self):
        self.committed = True
        if self._fail:
            raise RuntimeError("commit failed")


class FakeDb:
    def __init__(self, batch_fail: bool = False):
        self.batches: list[FakeBatch] = []
        self._batch_fail = batch_fail

    def batch(self):
        b = FakeBatch(fail=self._batch_fail)
        self.batches.append(b)
        return b

    def document(self, path: str):
        return path  # opaque ref; we only assert on path strings


class FakeStorage:
    """Storage stub exposing just download_as_bytes through services.gcs."""

    def __init__(self, blobs: dict[str, str]):
        self._blobs = blobs

    def bucket(self, name):
        client = self

        class Bucket:
            def blob(self, blob_name):
                key = f"{name}/{blob_name}"

                class Blob:
                    def download_as_bytes(self):
                        if key not in client._blobs:
                            raise FileNotFoundError(key)
                        return client._blobs[key].encode("utf-8")

                return Blob()

        return Bucket()


# ---------------------------------------------------------------------------
# _truthy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("true", True),
        ("True", True),
        ("yes", True),
        ("y", True),
        ("1", True),
        ("on", True),
        ("selected", True),
        ("false", False),
        ("rejected", False),
        ("", False),
        (None, False),
        (0, False),
        (1, True),
    ],
)
def test_truthy_parses_string_and_boolean_like_inputs(value, expected):
    assert _truthy(value) is expected


# ---------------------------------------------------------------------------
# read_selected_image_rows
# ---------------------------------------------------------------------------


def _shard_csv(rows: list[dict[str, str]]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def test_read_selected_image_rows_indexes_by_meta_image_uuid():
    storage = FakeStorage(
        {
            "ona-harvest/preprocessing/run-1/shard-1.csv": _shard_csv(
                [
                    {"meta_image_uuid": "img-a", "selected": "true"},
                    {"meta_image_uuid": "img-b", "selected": "false"},
                ]
            )
        }
    )

    rows = read_selected_image_rows(
        storage, ["gs://ona-harvest/preprocessing/run-1/shard-1.csv"]
    )

    assert set(rows) == {"img-a", "img-b"}
    assert rows["img-a"]["selected"] == "true"


def test_read_selected_image_rows_falls_back_to_image_path_stem():
    storage = FakeStorage(
        {
            "ona-harvest/preprocessing/run-1/shard-2.csv": _shard_csv(
                [
                    {
                        "meta_image_uuid": "",
                        "img_gcs_img_path": "x/y/derived-id.jpg",
                        "selected": "true",
                    }
                ]
            )
        }
    )

    rows = read_selected_image_rows(
        storage, ["gs://ona-harvest/preprocessing/run-1/shard-2.csv"]
    )

    assert "derived-id" in rows


def test_read_selected_image_rows_swallows_download_errors():
    storage = FakeStorage({})  # nothing exists; download will raise
    rows = read_selected_image_rows(storage, ["gs://nope/missing.csv"])
    assert rows == {}


# ---------------------------------------------------------------------------
# build_selected_image_prefixes
# ---------------------------------------------------------------------------


def test_build_selected_image_prefixes_uses_selected_images_bucket_when_copied():
    image_doc = make_image_doc(
        "doc-1",
        gcs_img_path="trial/sub/images/sub/image-uuid.jpg",
    )
    selected_rows = {
        "image-uuid": {
            "selected": "true",
            "copied": "true",
            "img_gcs_img_path": "trial/sub/images/sub/image-uuid.jpg",
            "img_bucket_prefix": "ona-harvest",
        }
    }

    selected_ids, prefixes = build_selected_image_prefixes(
        image_documents=[image_doc],
        selected_rows_by_image_id=selected_rows,
        selected_images_bucket="artemis-revamp",
    )

    assert "image-uuid" in selected_ids
    # When copied, the prefix uses the SELECTED_IMAGES_BUCKET, not the source.
    assert prefixes["doc-1"].startswith("gs://artemis-revamp/")
    assert prefixes["doc-1"].endswith("/")


def test_build_selected_image_prefixes_skips_unselected_rows():
    image_doc = make_image_doc("doc-1")
    selected_rows = {
        "image-uuid": {
            "selected": "false",
            "img_gcs_img_path": "a/b/image-uuid.jpg",
            "img_bucket_prefix": "ona-harvest",
        }
    }

    selected_ids, prefixes = build_selected_image_prefixes(
        image_documents=[image_doc],
        selected_rows_by_image_id=selected_rows,
        selected_images_bucket="artemis-revamp",
    )

    assert selected_ids == set()
    assert prefixes == {}


# ---------------------------------------------------------------------------
# write_back_preprocessing_status
# ---------------------------------------------------------------------------


def test_write_back_preprocessing_status_assigns_selected_or_rejected():
    db = FakeDb()
    docs = [make_image_doc(f"doc-{i}", gcs_img_path=f"a/b/img-{i}.jpg") for i in range(3)]
    selected = {"img-0", "img-2"}

    count = write_back_preprocessing_status(
        db,
        image_documents=docs,
        selected_image_ids=selected,
        preprocessing_success=True,
        batch_run_id="batch-1",
        run_id="run-1",
        logger=CaptureLogger(),
        trial_id="T--S",
        subtrial_id="2026--F--L",
    )

    assert count == 3
    all_writes = [w for batch in db.batches for w in batch.writes]
    statuses = {path: payload["preprocessing_status"] for path, payload in all_writes}
    assert "selected" in statuses.values()
    assert "rejected" in statuses.values()
    # The batch_run_id and run_id are propagated for successful preprocessing.
    sample_payload = all_writes[0][1]
    assert sample_payload["preprocessing_batch_run_id"] == "batch-1"
    assert sample_payload["preprocessing_run_id"] == "run-1"


def test_write_back_preprocessing_status_skipped_on_failure():
    db = FakeDb()
    docs = [make_image_doc("doc-1")]

    write_back_preprocessing_status(
        db,
        image_documents=docs,
        selected_image_ids=set(),
        preprocessing_success=False,
        batch_run_id="batch-1",
        run_id="run-1",
        logger=CaptureLogger(),
        trial_id="T--S",
        subtrial_id="2026--F--L",
    )

    write = db.batches[0].writes[0]
    assert write[1]["preprocessing_status"] == "skipped"
    # batch_run_id / run_id are NOT included on failure.
    assert "preprocessing_batch_run_id" not in write[1]


def test_write_back_preprocessing_status_swallows_commit_errors():
    db = FakeDb(batch_fail=True)
    docs = [make_image_doc("doc-1")]

    count = write_back_preprocessing_status(
        db,
        image_documents=docs,
        selected_image_ids={"image-uuid"},
        preprocessing_success=True,
        batch_run_id="batch-1",
        run_id="run-1",
        logger=CaptureLogger(),
        trial_id="T--S",
        subtrial_id="2026--F--L",
    )

    assert count == 0  # error swallowed, returns 0


# ---------------------------------------------------------------------------
# _batch_set chunking
# ---------------------------------------------------------------------------


def test_batch_set_chunks_payloads_by_400():
    db = FakeDb()
    payloads = [(f"path/{i}", {"i": i}) for i in range(900)]

    count = _batch_set(db, payloads)

    assert count == 900
    # 900 / 400 -> 3 chunks (sizes 400, 400, 100)
    assert [len(b.writes) for b in db.batches] == [400, 400, 100]
    assert all(b.committed for b in db.batches)
