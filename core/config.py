from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - python-dotenv is optional in Cloud Run
    load_dotenv = None


def _bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _int(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


def _float(value: str | None, default: float) -> float:
    if value is None or value == "":
        return default
    return float(value)


def _json_env(name: str) -> dict[str, Any] | None:
    raw = os.getenv(name)
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must contain service account JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{name} must contain a JSON object")
    return parsed


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value.strip()


@dataclass(frozen=True)
class AppConfig:
    gcs_bucket: str
    preprocessor_url: str
    inference_url: str
    gcp_project_id: str
    firestore_database_id: str = "artemis-prod"
    gcp_run_region: str = "us-central1"
    trait_extraction_job_id: str = "ona-trait-extraction"
    trait_extraction_runs_collection: str = "trait_extraction_runs"
    firebase_storage_bucket: str = "artemis-418513.firebasestorage.app"
    raw_prefix_root: str = "raw"
    inference_prefix_root: str = "inference"
    extraction_prefix_root: str = "extraction"
    selected_images_bucket: str = "artemis-revamp"
    preprocessor_poll_timeout_s: int = 14400
    preprocessor_request_timeout_s: int = 60
    preprocessor_transient_error_limit: int = 5
    inference_poll_timeout_s: int = 14400
    inference_request_timeout_s: int = 60
    trait_extraction_poll_timeout_s: int = 24 * 60 * 60
    trait_extraction_request_timeout_s: int = 60
    inference_confidence: float = 0.5
    inference_limit: int = 1_000_000
    use_cloud_logging: bool = True
    firebase_credentials: dict[str, Any] | None = None
    gcp_service_account_credentials: dict[str, Any] | None = None


def load_config(*, require_services: bool = True) -> AppConfig:
    """Load cron job configuration from environment without side effects."""
    if load_dotenv is not None:
        load_dotenv()

    preprocessor_url = (
        _required_env("PREPROCESSOR_URL")
        if require_services
        else os.getenv("PREPROCESSOR_URL", "http://localhost/pre_processing")
    )
    inference_url = (
        _required_env("INFERENCE_URL")
        if require_services
        else os.getenv("INFERENCE_URL", "http://localhost/ona_infer")
    )

    return AppConfig(
        gcs_bucket=os.getenv("GCS_BUCKET", "ona-harvest").strip() or "ona-harvest",
        preprocessor_url=preprocessor_url.rstrip("/"),
        inference_url=inference_url.rstrip("/"),
        gcp_project_id=os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or "artemis-418513",
        firestore_database_id=os.getenv("FIRESTORE_DATABASE_ID", "artemis-prod"),
        gcp_run_region=os.getenv("GCP_RUN_REGION", "us-central1"),
        trait_extraction_job_id=os.getenv("TRAIT_EXTRACTION_JOB_ID", "ona-trait-extraction"),
        trait_extraction_runs_collection=os.getenv(
            "TRAIT_EXTRACTION_RUNS_COLLECTION",
            "trait_extraction_runs",
        ),
        firebase_storage_bucket=os.getenv(
            "FIREBASE_STORAGE_BUCKET",
            "artemis-418513.firebasestorage.app",
        ),
        raw_prefix_root=os.getenv("RAW_PREFIX_ROOT", "raw").strip("/") or "raw",
        inference_prefix_root=os.getenv("INFERENCE_PREFIX_ROOT", "inference").strip("/") or "inference",
        extraction_prefix_root=os.getenv("EXTRACTION_PREFIX_ROOT", "extraction").strip("/") or "extraction",
        selected_images_bucket=os.getenv("SELECTED_IMAGES_BUCKET", "artemis-revamp"),
        preprocessor_poll_timeout_s=_int(os.getenv("PREPROCESSOR_POLL_TIMEOUT_SECONDS"), 14400),
        preprocessor_request_timeout_s=_int(os.getenv("PREPROCESSOR_REQUEST_TIMEOUT_SECONDS"), 60),
        preprocessor_transient_error_limit=_int(os.getenv("PREPROCESSOR_TRANSIENT_ERROR_LIMIT"), 5),
        inference_poll_timeout_s=_int(os.getenv("INFERENCE_POLL_TIMEOUT_SECONDS"), 14400),
        inference_request_timeout_s=_int(os.getenv("INFERENCE_REQUEST_TIMEOUT_SECONDS"), 60),
        trait_extraction_poll_timeout_s=_int(os.getenv("TRAIT_EXTRACTION_POLL_TIMEOUT_SECONDS"), 24 * 60 * 60),
        trait_extraction_request_timeout_s=_int(os.getenv("TRAIT_EXTRACTION_REQUEST_TIMEOUT_SECONDS"), 60),
        inference_confidence=_float(os.getenv("INFERENCE_CONFIDENCE"), 0.5),
        inference_limit=_int(os.getenv("INFERENCE_LIMIT"), 1_000_000),
        use_cloud_logging=_bool(os.getenv("USE_CLOUD_LOGGING"), True),
        firebase_credentials=_json_env("FIREBASE_CREDENTIALS"),
        gcp_service_account_credentials=_json_env("GCP_SERVICE_ACCOUNT_CREDENTIALS"),
    )
