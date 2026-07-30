"""Structured JSON logging to stdout.

Every log entry is a JSON object containing at minimum:
  run_id, stage, status, timestamp (ISO 8601 UTC).

On Databricks these lines are captured in the driver / job run logs.
"""

from __future__ import annotations

import json
import sys
import traceback
from datetime import datetime, timezone
from typing import Any


class StructuredLogger:
    """Emits structured JSON log entries to stdout."""

    def __init__(self, run_id: str) -> None:
        self._run_id = run_id

    def log(
        self,
        stage: str,
        status: str,
        *,
        run_id: str | None = None,
        job_id: str | None = None,
        errors: str | dict | None = None,
        exc: BaseException | None = None,
        **extra: Any,
    ) -> None:
        """Emit a structured log entry.

        Args:
            stage: Pipeline stage name (e.g. "batch_inference").
            status: Status string (e.g. "started", "success", "failed").
            run_id: Optional pipeline run identifier (overrides the bound one).
            job_id: Optional Databricks job identifier.
            errors: Optional error payload (string or dict).
            exc: Optional caught exception; populates ``errors`` with the
                 message and traceback.
            **extra: Any additional fields to include in the log entry.
        """
        entry: dict[str, Any] = {
            "run_id": run_id or self._run_id,
            "stage": stage,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if job_id is not None:
            entry["job_id"] = job_id

        if exc is not None:
            entry["errors"] = {
                "error_message": str(exc),
                "traceback": traceback.format_exc(),
            }
        elif errors is not None:
            entry["errors"] = {"error_message": errors} if isinstance(errors, str) else errors
        else:
            entry["errors"] = None

        entry.update({k: v for k, v in extra.items() if v is not None})

        print(json.dumps(entry, default=str), file=sys.stdout, flush=True)

    def log_exception(self, stage: str, status: str, exc: BaseException, **extra: Any) -> None:
        self.log(stage, status, exc=exc, **extra)


def get_logger(run_id: str) -> StructuredLogger:
    """Create a StructuredLogger bound to the given run_id."""
    return StructuredLogger(run_id=run_id)
