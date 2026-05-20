from __future__ import annotations

import time
from typing import Callable

import httpx

from cron_job.core.config import AppConfig
from cron_job.gcs_client import parse_gcs_uri
from cron_job.models import PreprocessorResult


TERMINAL_SUCCESS = {"success", "completed", "complete"}
TERMINAL_FAILED = {"failed", "error", "cancelled", "canceled"}


def _backoff(attempt: int) -> int:
    return min(10 * (2 ** max(0, attempt - 1)), 60)


def raw_prefix_from_csv_uri(uri: str) -> str:
    _, blob_name = parse_gcs_uri(uri)
    return blob_name


def _selected_shards_from_firestore(db, batch_run_id: str) -> list[str]:
    if db is None:
        return []
    selected: list[str] = []

    try:
        snaps = list(
            db.collection("preprocessing_runs")
            .document(batch_run_id)
            .collection("shards")
            .stream()
        )
    except Exception:
        snaps = []

    def collect(data: dict) -> None:
        uri = data.get("selected_uri")
        status = data.get("selection_status") or data.get("status")
        if uri and status in {"selected", "success"}:
            selected.append(str(uri))

    for snap in snaps:
        collect(snap.to_dict() or {})

    try:
        run_snap = db.collection("preprocessing_runs").document(batch_run_id).get()
        legacy_shards = (run_snap.to_dict() or {}).get("shards") or {}
    except Exception:
        legacy_shards = {}
    if isinstance(legacy_shards, dict):
        for shard_info in legacy_shards.values():
            if isinstance(shard_info, dict):
                collect(shard_info)

    return list(dict.fromkeys(selected))


def selected_shards_from_firestore(db, batch_run_id: str) -> list[str]:
    return _selected_shards_from_firestore(db, batch_run_id)


def run_preprocessing(
    config: AppConfig,
    *,
    db,
    image_csv_uri: str,
    batch_run_id: str,
    logger,
    sleep: Callable[[float], None] = time.sleep,
) -> PreprocessorResult:
    raw_prefix = "raw"
    payload = {
        "src_bucket": config.gcs_bucket,
        "raw_prefix": raw_prefix,
        "batch_run_id": batch_run_id,
    }

    try:
        with httpx.Client(timeout=config.preprocessor_request_timeout_s) as client:
            response = client.post(f"{config.preprocessor_url}/start", json=payload)
        if response.status_code >= 400:
            logger.log(
                "preprocessing",
                "failed",
                batch_run_id=batch_run_id,
                http_status=response.status_code,
                response_body=response.text,
            )
            return PreprocessorResult(False, batch_run_id, "failed", error=response.text)
    except Exception as exc:
        logger.log("preprocessing", "failed", batch_run_id=batch_run_id, exc=exc)
        return PreprocessorResult(False, batch_run_id, "failed", error=str(exc))

    started = time.monotonic()
    transient_errors = 0
    attempt = 1

    while True:
        elapsed = time.monotonic() - started
        if elapsed > config.preprocessor_poll_timeout_s:
            logger.log("preprocessing", "timeout", batch_run_id=batch_run_id)
            return PreprocessorResult(False, batch_run_id, "timeout", error="poll timeout")

        sleep(_backoff(attempt))
        attempt += 1

        try:
            with httpx.Client(timeout=config.preprocessor_request_timeout_s) as client:
                response = client.get(f"{config.preprocessor_url}/status/{batch_run_id}")
            if response.status_code >= 400:
                raise RuntimeError(f"status HTTP {response.status_code}: {response.text}")
            body = response.json()
            status = str(body.get("status") or "").lower()
            transient_errors = 0
        except Exception as exc:
            transient_errors += 1
            if transient_errors >= config.preprocessor_transient_error_limit:
                logger.log(
                    "preprocessing",
                    "failed",
                    batch_run_id=batch_run_id,
                    exc=exc,
                    transient_errors=transient_errors,
                )
                return PreprocessorResult(False, batch_run_id, "failed", error=str(exc))
            continue

        if status in TERMINAL_SUCCESS:
            selected_shards = _selected_shards_from_firestore(db, batch_run_id)
            logger.log(
                "preprocessing",
                "success",
                batch_run_id=batch_run_id,
                selected_shard_count=len(selected_shards),
            )
            return PreprocessorResult(True, batch_run_id, "success", selected_shard_uris=selected_shards)
        if status in TERMINAL_FAILED:
            logger.log(
                "preprocessing",
                "failed",
                batch_run_id=batch_run_id,
                response_body=body,
            )
            return PreprocessorResult(False, batch_run_id, "failed", error=str(body))
