"""Tests for services/csv/writeback.py.

Covers:
- Joining extraction outputs onto raw CSV rows by document_id / plot_uid
- Preserving original column order
- Suffixing extraction columns with the canonical trait name
- Excluded source columns are not appended
- Failure modes (raw CSV download fails, no extraction outputs)
"""
from __future__ import annotations

import csv
import io

from cron_job.schemas.models import ExtractionRunResult
from cron_job.services.csv.writeback import write_back_csv

from .conftest import CaptureLogger


# ---------------------------------------------------------------------------
# Fake storage client that holds in-memory text blobs
# ---------------------------------------------------------------------------


class InMemoryStorage:
    def __init__(self, blobs: dict[str, str] | None = None):
        # key: "bucket/blob_name", value: text content
        self._blobs: dict[str, str] = dict(blobs or {})
        self.uploaded: list[tuple[str, str]] = []

    def bucket(self, name):
        client = self

        class Bucket:
            def blob(self, blob_name):
                return _Blob(client, name, blob_name)

            def list_blobs(self, prefix):
                key_prefix = f"{name}/{prefix}"
                return [
                    type("B", (), {"name": key.split("/", 1)[1]})()
                    for key in client._blobs
                    if key.startswith(key_prefix)
                ]

        return Bucket()


class _Blob:
    def __init__(self, client: InMemoryStorage, bucket: str, blob: str):
        self._client = client
        self._key = f"{bucket}/{blob}"

    def download_as_bytes(self):
        if self._key not in self._client._blobs:
            raise FileNotFoundError(self._key)
        return self._client._blobs[self._key].encode("utf-8")

    def upload_from_string(self, data, content_type=None):
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        self._client._blobs[self._key] = data
        self._client.uploaded.append((self._key, data))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_csv() -> str:
    return (
        "collection_name,document_id,plot_uid,protocol,trait\n"
        "flowering_data,doc-1,plot-1,manual,Flowering Date\n"
        "flowering_data,doc-2,plot-2,manual,Flowering Date\n"
    )


def _result_csv() -> str:
    return (
        "plot_uid,trial_name,site_name,score,confidence\n"
        "plot-1,T,S,5,0.9\n"
        "plot-2,T,S,7,0.8\n"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_writeback_appends_trait_suffixed_columns_and_preserves_raw_order():
    storage = InMemoryStorage(
        {
            "ona-harvest/raw/run-1/sub_classical.csv": _raw_csv(),
            # plot-level CSV under the extraction prefix
            "ona-harvest/extraction/run-1/sub/classical/flowering/extraction_outputs/plot-level-results.csv": _result_csv(),
        }
    )
    extraction = ExtractionRunResult(
        method="classical",
        canonical_trait_name="flowering",
        success=True,
        run_id="audit-1",
        output_prefix="gs://ona-harvest/extraction/run-1/sub/classical/flowering/extraction_outputs/",
        status="completed",
    )

    ok = write_back_csv(
        storage,
        raw_csv_uri="gs://ona-harvest/raw/run-1/sub_classical.csv",
        extraction_results=[extraction],
        csv_type="classical",
        logger=CaptureLogger(),
        trial_id="T--S",
        subtrial_id="2026--F--L",
    )

    assert ok is True
    written = storage._blobs["ona-harvest/raw/run-1/sub_classical.csv"]
    reader = csv.DictReader(io.StringIO(written))
    columns = reader.fieldnames or []

    # Raw columns preserved in original order at the head.
    assert columns[:5] == [
        "collection_name",
        "document_id",
        "plot_uid",
        "protocol",
        "trait",
    ]
    # Extraction columns are appended with __<canonical_trait> suffix.
    assert "score__flowering" in columns
    assert "confidence__flowering" in columns
    # Excluded source columns (trial_name, site_name) are NOT appended.
    assert "trial_name__flowering" not in columns
    assert "site_name__flowering" not in columns

    rows = list(reader)
    by_doc = {r["document_id"]: r for r in rows}
    assert by_doc["doc-1"]["score__flowering"] == "5"
    assert by_doc["doc-1"]["confidence__flowering"] == "0.9"
    assert by_doc["doc-2"]["score__flowering"] == "7"


def test_writeback_returns_false_when_raw_csv_unreadable():
    storage = InMemoryStorage()  # raw CSV not present
    ok = write_back_csv(
        storage,
        raw_csv_uri="gs://ona-harvest/raw/missing.csv",
        extraction_results=[],
        csv_type="classical",
        logger=CaptureLogger(),
        trial_id="T--S",
        subtrial_id="2026--F--L",
    )
    assert ok is False


def test_writeback_succeeds_with_no_extraction_outputs():
    """When no extraction outputs are present, raw CSV is rewritten unchanged."""
    storage = InMemoryStorage(
        {"ona-harvest/raw/run-1/sub_classical.csv": _raw_csv()}
    )
    ok = write_back_csv(
        storage,
        raw_csv_uri="gs://ona-harvest/raw/run-1/sub_classical.csv",
        extraction_results=[],
        csv_type="classical",
        logger=CaptureLogger(),
        trial_id="T--S",
        subtrial_id="2026--F--L",
    )
    assert ok is True
    # The columns and rows match the raw input.
    written = storage._blobs["ona-harvest/raw/run-1/sub_classical.csv"]
    reader = csv.DictReader(io.StringIO(written))
    assert reader.fieldnames == [
        "collection_name",
        "document_id",
        "plot_uid",
        "protocol",
        "trait",
    ]


def test_writeback_skips_failed_extraction_results():
    storage = InMemoryStorage(
        {
            "ona-harvest/raw/run-1/sub_classical.csv": _raw_csv(),
            "ona-harvest/extraction/run-1/sub/classical/flowering/extraction_outputs/plot-level-results.csv": _result_csv(),
        }
    )
    failed = ExtractionRunResult(
        method="classical",
        canonical_trait_name="flowering",
        success=False,
        run_id="audit-1",
        output_prefix="gs://ona-harvest/extraction/run-1/sub/classical/flowering/extraction_outputs/",
        status="failed",
    )

    ok = write_back_csv(
        storage,
        raw_csv_uri="gs://ona-harvest/raw/run-1/sub_classical.csv",
        extraction_results=[failed],
        csv_type="classical",
        logger=CaptureLogger(),
        trial_id="T--S",
        subtrial_id="2026--F--L",
    )

    assert ok is True
    # No new columns should have been appended for the failed result.
    written = storage._blobs["ona-harvest/raw/run-1/sub_classical.csv"]
    reader = csv.DictReader(io.StringIO(written))
    assert "score__flowering" not in (reader.fieldnames or [])
