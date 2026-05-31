"""Tests for the StructuredLogger emitted by middleware/logger.py.

Asserts the required JSON schema (run_id / stage / status / timestamp), error
field shaping, and that arbitrary kwargs are merged into the log entry.
"""
from __future__ import annotations

import json

import pytest

from cron_job.middleware.logger import StructuredLogger, get_logger


class CaptureCloudLogger:
    """Stand-in for google.cloud.logging.Logger that captures log_struct calls."""

    def __init__(self):
        self.entries: list[dict] = []

    def log_struct(self, entry):
        self.entries.append(entry)


def test_log_emits_required_fields():
    cloud = CaptureCloudLogger()
    logger = StructuredLogger("run-xyz", cloud_logger=cloud)

    logger.log("discovery", "complete", document_count=5)

    assert len(cloud.entries) == 1
    entry = cloud.entries[0]
    for key in ("run_id", "stage", "status", "timestamp"):
        assert key in entry
    assert entry["run_id"] == "run-xyz"
    assert entry["stage"] == "discovery"
    assert entry["status"] == "complete"
    assert entry["document_count"] == 5


def test_log_serializes_extra_kwargs():
    cloud = CaptureCloudLogger()
    logger = StructuredLogger("run-1", cloud_logger=cloud)

    logger.log("custom_stage", "ok", custom_key="value", another=42)

    entry = cloud.entries[0]
    assert entry["custom_key"] == "value"
    assert entry["another"] == 42


def test_log_exception_populates_errors_field_with_message_and_traceback():
    cloud = CaptureCloudLogger()
    logger = StructuredLogger("run-1", cloud_logger=cloud)

    try:
        raise ValueError("specific failure detail")
    except ValueError as exc:
        logger.log_exception("scanning", "failed", exc, subtrial_id="2026--F--L")

    entry = cloud.entries[0]
    assert entry["status"] == "failed"
    assert entry["errors"]["error_message"] == "specific failure detail"
    assert "traceback" in entry["errors"]


def test_log_with_string_errors_wraps_into_dict():
    cloud = CaptureCloudLogger()
    logger = StructuredLogger("run-1", cloud_logger=cloud)

    logger.log("scanning", "warning", errors="missing field x")

    assert cloud.entries[0]["errors"] == {"error_message": "missing field x"}


def test_log_falls_back_to_stdout_when_no_cloud_logger(capsys):
    logger = get_logger("run-1")
    logger.log("smoke_test", "success")

    out = capsys.readouterr().out.strip()
    payload = json.loads(out.splitlines()[-1])
    assert payload["run_id"] == "run-1"
    assert payload["stage"] == "smoke_test"
    assert payload["status"] == "success"


@pytest.mark.parametrize("status", ["complete", "failed", "fatal", "skipped"])
def test_log_records_arbitrary_status_strings(status):
    cloud = CaptureCloudLogger()
    logger = StructuredLogger("run-1", cloud_logger=cloud)

    logger.log("any_stage", status)

    assert cloud.entries[0]["status"] == status
