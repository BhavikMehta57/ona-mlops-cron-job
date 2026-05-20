"""
Structured JSON logging to Google Cloud Logging.

Every log entry is a JSON object containing at minimum:
  run_id, stage, status, timestamp (ISO 8601 UTC).
"""

from __future__ import annotations

import traceback
from datetime import datetime, timezone
from typing import Any

try:
    import google.cloud.logging as cloud_logging
    from google.cloud.logging import Client as CloudLoggingClient
    _CLOUD_LOGGING_AVAILABLE = True
except ImportError:
    _CLOUD_LOGGING_AVAILABLE = False


class StructuredLogger:
    """Emits structured JSON log entries to Google Cloud Logging."""

    def __init__(self, run_id: str, cloud_logger: Any | None = None) -> None:
        self._run_id = run_id
        self._cloud_logger = cloud_logger

    def log(
        self,
        stage: str,
        status: str,
        *,
        trial_id: str | None = None,
        subtrial_id: str | None = None,
        document_count: int | None = None,
        batch_run_id: str | None = None,
        trait_type: str | None = None,
        job_id: str | None = None,
        output_gcs_path: str | None = None,
        subtrial_index: int | None = None,
        errors: str | dict | None = None,
        exc: BaseException | None = None,
        **extra: Any,
    ) -> None:
        """Emit a structured log entry.

        Args:
            stage: Pipeline stage name (e.g. "discovery", "scanning").
            status: Status string (e.g. "complete", "failed", "fatal").
            trial_id: Optional trial identifier.
            subtrial_id: Optional subtrial identifier.
            document_count: Optional document count.
            batch_run_id: Optional batch run identifier.
            trait_type: Optional trait type string.
            job_id: Optional inference job identifier.
            output_gcs_path: Optional GCS output path.
            subtrial_index: Optional 1-based subtrial position index.
            errors: Optional error payload (string or dict).
            exc: Optional caught exception; if provided, populates ``errors``
                 with both ``error_message`` and ``traceback``.
            **extra: Any additional fields to include in the log entry.
        """
        entry: dict[str, Any] = {
            "run_id": self._run_id,
            "stage": stage,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if trial_id is not None:
            entry["trial_id"] = trial_id
        if subtrial_id is not None:
            entry["subtrial_id"] = subtrial_id
        if document_count is not None:
            entry["document_count"] = document_count
        if batch_run_id is not None:
            entry["batch_run_id"] = batch_run_id
        if trait_type is not None:
            entry["trait_type"] = trait_type
        if job_id is not None:
            entry["job_id"] = job_id
        if output_gcs_path is not None:
            entry["output_gcs_path"] = output_gcs_path
        if subtrial_index is not None:
            entry["subtrial_index"] = subtrial_index

        # Populate errors field
        if exc is not None:
            entry["errors"] = {
                "error_message": str(exc),
                "traceback": traceback.format_exc(),
            }
        elif errors is not None:
            if isinstance(errors, str):
                entry["errors"] = {"error_message": errors}
            else:
                entry["errors"] = errors
        else:
            entry["errors"] = None

        # Merge any extra fields
        entry.update(extra)

        if self._cloud_logger is not None:
            self._cloud_logger.log_struct(entry)
        else:
            import json
            import sys
            print(json.dumps(entry), file=sys.stdout, flush=True)

    def log_exception(self, stage: str, status: str, exc: BaseException, **extra: Any) -> None:
        self.log(stage, status, exc=exc, **extra)


def get_logger(run_id: str, cloud_logger: Any | None = None) -> StructuredLogger:
    """Create a StructuredLogger bound to the given run_id.

    Args:
        run_id: The unique run identifier for this job execution.
        cloud_logger: Optional Google Cloud Logging logger instance.
                      If None, log entries are printed to stdout as JSON.

    Returns:
        A configured StructuredLogger instance.
    """
    return StructuredLogger(run_id=run_id, cloud_logger=cloud_logger)
