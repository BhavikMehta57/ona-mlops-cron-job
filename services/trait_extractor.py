from __future__ import annotations

import json
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

import httpx
from firebase_admin import firestore
from google.auth.transport.requests import Request

from cron_job.core.config import AppConfig
from cron_job.services.gcs import make_gcs_uri
from cron_job.schemas.models import ExtractionRunResult, InferenceJobResult, ProtocolTraitGroup

try:
    import google.auth
    from google.oauth2 import service_account
except ImportError:  # pragma: no cover
    google = None
    service_account = None


TERMINAL = {"completed", "failed", "cancelled", "canceled"}


@dataclass(frozen=True)
class CloudRunJobRef:
    project: str
    region: str
    job_id: str

    @property
    def job_name(self) -> str:
        return f"projects/{self.project}/locations/{self.region}/jobs/{self.job_id}"


class CloudRunJobClient:
    def __init__(self, config: AppConfig):
        self.ref = CloudRunJobRef(
            project=config.gcp_project_id,
            region=config.gcp_run_region,
            job_id=config.trait_extraction_job_id,
        )
        self._base = "https://run.googleapis.com/v2"
        self._request = Request()
        self._http = httpx.Client(timeout=config.trait_extraction_request_timeout_s)
        if config.gcp_service_account_credentials and service_account is not None:
            self._creds = service_account.Credentials.from_service_account_info(
                config.gcp_service_account_credentials,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
        else:
            if google is None:
                raise RuntimeError("google-auth is required for Cloud Run Jobs access")
            self._creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

    def _headers(self) -> dict[str, str]:
        if not self._creds.valid:
            self._creds.refresh(self._request)
        return {"Authorization": f"Bearer {self._creds.token}"}

    def run_job_with_inline_spec(self, run_spec: dict[str, Any]) -> dict[str, Any]:
        spec_str = json.dumps(run_spec, separators=(",", ":"), ensure_ascii=True)
        body = {
            "overrides": {
                "containerOverrides": [
                    {
                        "args": [
                            "-m",
                            "trait_extraction.runner.entrypoint",
                            "--run-spec-json",
                            spec_str,
                        ]
                    }
                ]
            }
        }
        response = self._http.post(f"{self._base}/{self.ref.job_name}:run", headers=self._headers(), json=body)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(f"Cloud Run Jobs API error {response.status_code}: {response.text}") from exc
        return response.json()

    def get_operation(self, operation_name: str) -> dict[str, Any]:
        response = self._http.get(f"{self._base}/{operation_name}", headers=self._headers())
        response.raise_for_status()
        return response.json()

    def get_execution(self, execution_name: str) -> dict[str, Any]:
        response = self._http.get(f"{self._base}/{execution_name}", headers=self._headers())
        response.raise_for_status()
        return response.json()


def _execution_status(execution: dict[str, Any]) -> tuple[str, dict[str, int], str | None, str | None, str | None]:
    counts = {
        "taskCount": int(execution.get("taskCount") or 0),
        "succeededCount": int(execution.get("succeededCount") or 0),
        "failedCount": int(execution.get("failedCount") or 0),
        "cancelledCount": int(execution.get("cancelledCount") or 0),
    }
    start_time = execution.get("startTime")
    completion_time = execution.get("completionTime")
    log_uri = execution.get("logUri")
    if completion_time:
        if counts["failedCount"] > 0:
            return "failed", counts, start_time, completion_time, log_uri
        if counts["cancelledCount"] > 0:
            return "cancelled", counts, start_time, completion_time, log_uri
        return "completed", counts, start_time, completion_time, log_uri
    if counts["cancelledCount"] > 0:
        return "cancel_requested", counts, start_time, None, log_uri
    if start_time:
        return "processing", counts, start_time, None, log_uri
    return "enqueued", counts, None, None, log_uri


def _run_ref(db, collection: str, run_id: str):
    return db.collection(collection).document(run_id)


def _create_run_record(db, collection: str, run_id: str, run_spec: dict[str, Any], job_name: str, op_name: str) -> None:
    now = firestore.SERVER_TIMESTAMP
    _run_ref(db, collection, run_id).set(
        {
            "run_id": run_id,
            "created_at": now,
            "updated_at": now,
            "run_spec": run_spec,
            "job_name": job_name,
            "operation_name": op_name,
            "execution_name": None,
            "status": "enqueued",
            "counts": None,
            "start_time": None,
            "completion_time": None,
            "log_uri": None,
            "last_polled_at": None,
        }
    )


def _update_run_record(db, collection: str, run_id: str, patch: dict[str, Any]) -> None:
    patch = dict(patch)
    patch["updated_at"] = firestore.SERVER_TIMESTAMP
    _run_ref(db, collection, run_id).set(patch, merge=True)


def _slug(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip())
    return value.strip("-") or "default"


def _output_prefix(
    config: AppConfig,
    run_id: str,
    subtrial_id: str,
    method: str,
    trait: str,
    qualifier: str | None = None,
) -> str:
    suffix = f"{trait}/{_slug(qualifier)}" if qualifier else trait
    if method == "classical":
        path = f"{config.extraction_prefix_root}/{run_id}/{subtrial_id}/classical/{suffix}/extraction_outputs/"
    else:
        path = f"{config.extraction_prefix_root}/{run_id}/{subtrial_id}/{suffix}/extraction_outputs/"
    return make_gcs_uri(config.gcs_bucket, path)


def _poll_execution(
    config: AppConfig,
    db,
    cr: CloudRunJobClient,
    *,
    audit_run_id: str,
    operation_name: str,
    logger,
    sleep: Callable[[float], None],
) -> tuple[str, str | None, str | None]:
    started = time.monotonic()
    execution_name: str | None = None

    while True:
        if time.monotonic() - started > config.trait_extraction_poll_timeout_s:
            return "timeout", execution_name, "poll timeout"

        if not execution_name:
            op = cr.get_operation(operation_name)
            if op.get("done") and isinstance(op.get("response"), dict):
                execution_name = op["response"].get("name")
                if execution_name:
                    _update_run_record(
                        db,
                        config.trait_extraction_runs_collection,
                        audit_run_id,
                        {"execution_name": execution_name},
                    )
            elif isinstance(op.get("metadata"), dict):
                execution_name = op["metadata"].get("name")
                if execution_name:
                    _update_run_record(
                        db,
                        config.trait_extraction_runs_collection,
                        audit_run_id,
                        {"execution_name": execution_name},
                    )

        if execution_name:
            execution = cr.get_execution(execution_name)
            status, counts, start_time, completion_time, log_uri = _execution_status(execution)
            _update_run_record(
                db,
                config.trait_extraction_runs_collection,
                audit_run_id,
                {
                    "status": status,
                    "counts": counts,
                    "start_time": start_time,
                    "completion_time": completion_time,
                    "log_uri": log_uri,
                    "last_polled_at": firestore.SERVER_TIMESTAMP,
                },
            )
            if status in TERMINAL:
                return status, execution_name, None

        sleep(15)


def run_one_extraction(
    config: AppConfig,
    *,
    db,
    cloud_run_client: CloudRunJobClient,
    run_spec: dict[str, Any],
    method: str,
    trait: str,
    output_prefix: str,
    logger,
    subtrial_id: str,
    sleep: Callable[[float], None] = time.sleep,
) -> ExtractionRunResult:
    audit_run_id = uuid.uuid4().hex
    try:
        op = cloud_run_client.run_job_with_inline_spec(run_spec)
        operation_name = op.get("name")
        if not operation_name:
            raise RuntimeError("Cloud Run returned operation without name")
        _create_run_record(
            db,
            config.trait_extraction_runs_collection,
            audit_run_id,
            run_spec,
            cloud_run_client.ref.job_name,
            operation_name,
        )
        execution_name = None
        if isinstance(op.get("metadata"), dict):
            execution_name = op["metadata"].get("name")
            if execution_name:
                _update_run_record(
                    db,
                    config.trait_extraction_runs_collection,
                    audit_run_id,
                    {"execution_name": execution_name},
                )

        status, final_execution_name, error = _poll_execution(
            config,
            db,
            cloud_run_client,
            audit_run_id=audit_run_id,
            operation_name=operation_name,
            logger=logger,
            sleep=sleep,
        )
        execution_name = final_execution_name or execution_name
        success = status == "completed"
        logger.log(
            "trait_extraction",
            "success" if success else status,
            subtrial_id=subtrial_id,
            method=method,
            trait=trait,
            output_prefix=output_prefix,
            trait_extraction_run_id=audit_run_id,
            execution_name=execution_name,
        )
        return ExtractionRunResult(
            method=method,
            canonical_trait_name=trait,
            success=success,
            run_id=audit_run_id,
            output_prefix=output_prefix,
            status=status,
            operation_name=operation_name,
            execution_name=execution_name,
            error=error,
        )
    except Exception as exc:
        logger.log(
            "trait_extraction",
            "failed",
            subtrial_id=subtrial_id,
            method=method,
            trait=trait,
            output_prefix=output_prefix,
            exc=exc,
        )
        return ExtractionRunResult(method, trait, False, audit_run_id, output_prefix, "failed", error=str(exc))


def run_cv_extractions(
    config: AppConfig,
    *,
    db,
    cloud_run_client: CloudRunJobClient,
    run_id: str,
    subtrial_id: str,
    inference_results: list[InferenceJobResult],
    logger,
    planting_date: str | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> list[ExtractionRunResult]:
    seen: set[tuple[str, str]] = set()
    results: list[ExtractionRunResult] = []
    for inference in inference_results:
        if not inference.success or not inference.output_gcs_path:
            continue
        key = (inference.canonical_trait_name, inference.output_gcs_path)
        if key in seen:
            continue
        seen.add(key)
        run_spec = {
            "trait": inference.canonical_trait_name,
            "method": "computer_vision",
            "input_dir": inference.output_gcs_path,
            "recursive": True,
        }
        if inference.canonical_trait_name == "flowering" and planting_date:
            run_spec["planting_date"] = planting_date
        results.append(
            run_one_extraction(
                config,
                db=db,
                cloud_run_client=cloud_run_client,
                run_spec=run_spec,
                method="computer_vision",
                trait=inference.canonical_trait_name,
                output_prefix=inference.output_gcs_path,
                logger=logger,
                subtrial_id=subtrial_id,
                sleep=sleep,
            )
        )
    return results


def run_classical_extractions(
    config: AppConfig,
    *,
    db,
    cloud_run_client: CloudRunJobClient,
    run_id: str,
    subtrial_id: str,
    input_csv: str,
    groups: list[ProtocolTraitGroup],
    planting_date: str | None,
    logger,
    sleep: Callable[[float], None] = time.sleep,
) -> list[ExtractionRunResult]:
    results: list[ExtractionRunResult] = []
    for group in groups:
        run_spec: dict[str, Any] = {
            "trait": group.canonical_trait_name,
            "method": "classical",
            "input_csv": input_csv,
            "recursive": True,
        }
        if group.canonical_trait_name == "flowering" and planting_date:
            run_spec["planting_date"] = planting_date
        results.append(
            run_one_extraction(
                config,
                db=db,
                cloud_run_client=cloud_run_client,
                run_spec=run_spec,
                method="classical",
                trait=group.canonical_trait_name,
                output_prefix=input_csv,
                logger=logger,
                subtrial_id=subtrial_id,
                sleep=sleep,
            )
        )
    return results
