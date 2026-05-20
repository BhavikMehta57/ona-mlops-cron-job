"""Shared fixtures and helpers for cron_job tests."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from cron_job.core.config import AppConfig
from cron_job.schemas.models import (
    InferenceJobResult,
    ProtocolTraitGroup,
    ScannedDocument,
    SubtrialInfo,
)


class CaptureLogger:
    """Captures structured log entries in-memory for assertions."""

    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []

    def log(self, stage: str, status: str, **extra: Any) -> None:
        self.entries.append({"stage": stage, "status": status, **extra})

    def log_exception(self, stage: str, status: str, exc: BaseException, **extra: Any) -> None:
        self.log(stage, status, exc=exc, **extra)

    def find(self, *, stage: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        return [
            entry
            for entry in self.entries
            if (stage is None or entry["stage"] == stage)
            and (status is None or entry["status"] == status)
        ]


@pytest.fixture
def logger() -> CaptureLogger:
    return CaptureLogger()


@pytest.fixture
def app_config() -> AppConfig:
    return AppConfig(
        gcs_bucket="ona-harvest",
        preprocessor_url="http://preprocessor",
        inference_url="http://inference",
        gcp_project_id="project",
        use_cloud_logging=False,
        preprocessor_poll_timeout_s=30,
        inference_poll_timeout_s=30,
    )


@pytest.fixture
def utc_now() -> datetime:
    return datetime(2026, 5, 14, 12, tzinfo=timezone.utc)


def make_image_doc(
    document_id: str = "img-1",
    *,
    trait: str = "Pod Count",
    protocol: str = "RGB",
    bucket_prefix: str = "ona-harvest",
    gcs_img_path: str = "a/b/image-uuid.jpg",
    extra_fields: dict[str, Any] | None = None,
    upload_ts: datetime | None = None,
) -> ScannedDocument:
    upload_ts = upload_ts or datetime(2026, 5, 14, 12, tzinfo=timezone.utc)
    fields: dict[str, Any] = {
        "bucket_prefix": bucket_prefix,
        "gcs_img_path": gcs_img_path,
        "plot_uid": "plot-1",
        "upload_timestamp": upload_ts,
    }
    if extra_fields:
        fields.update(extra_fields)
    return ScannedDocument(
        collection_name="images",
        document_id=document_id,
        collection_path=f"trials/t/subtrials/s/images/p/plot/{document_id}",
        protocol_date_id=f"{protocol}--2026-05-14",
        image_uri=f"gs://{bucket_prefix}/{gcs_img_path}",
        protocol=protocol,
        trait=trait,
        upload_timestamp=upload_ts,
        fields=fields,
    )


def make_classical_doc(
    document_id: str = "doc-1",
    *,
    collection_name: str = "flowering_data",
    trait: str = "Flowering Date",
    protocol: str = "manual",
    extra_fields: dict[str, Any] | None = None,
    upload_ts: datetime | None = None,
    data_collection: str | None = None,
) -> ScannedDocument:
    upload_ts = upload_ts or datetime(2026, 5, 14, 12, tzinfo=timezone.utc)
    fields: dict[str, Any] = {
        "project_name": "Artemis",
        "site_name": "Arusha--CIAT",
        "trial_name": "mvp-validation",
        "season": "2025--TZA--Bushbean--November",
        "field": "TARI Selian",
        "location": "Arusha",
        "protocol": protocol,
        "trait": trait,
        "plot_uid": f"plot-{document_id}",
        "upload_timestamp": upload_ts,
    }
    if data_collection:
        fields["data_collection"] = data_collection
    if extra_fields:
        fields.update(extra_fields)
    return ScannedDocument(
        collection_name=collection_name,
        document_id=document_id,
        collection_path=f"trials/t/subtrials/s/{collection_name}/{document_id}",
        protocol_date_id=None,
        image_uri=None,
        protocol=protocol,
        trait=trait,
        upload_timestamp=upload_ts,
        fields=fields,
    )


def make_subtrial_info(
    *,
    trial_id: str = "T--S",
    subtrial_id: str = "2026--F--L",
    planting_date: str | None = None,
) -> SubtrialInfo:
    subtrial_data: dict[str, Any] = {}
    if planting_date:
        subtrial_data["planting_date"] = planting_date
    return SubtrialInfo(
        trial_id=trial_id,
        subtrial_id=subtrial_id,
        trial_layout_id="layout",
        trial_data={},
        subtrial_data=subtrial_data,
        layout_data={},
    )


def make_inference_result(
    *,
    canonical_trait: str = "pods",
    inference_trait_type: str = "pod",
    output_path: str = "gs://ona-harvest/inference/run-1/sub/pods/RGB/json/",
    success: bool = True,
    job_id: str = "job-1",
) -> InferenceJobResult:
    return InferenceJobResult(
        protocol="RGB",
        raw_trait_value=canonical_trait,
        canonical_trait_name=canonical_trait,  # type: ignore[arg-type]
        inference_trait_type=inference_trait_type,
        success=success,
        job_id=job_id,
        output_gcs_path=output_path,
    )


def make_group(
    *,
    canonical_trait: str = "pods",
    inference_trait_type: str = "pod",
    image_prefixes: list[str] | None = None,
    document_ids: list[str] | None = None,
) -> ProtocolTraitGroup:
    return ProtocolTraitGroup(
        protocol="RGB",
        raw_trait_value=canonical_trait,
        canonical_trait_name=canonical_trait,  # type: ignore[arg-type]
        inference_trait_type=inference_trait_type,
        source_document_ids=document_ids or ["doc-1"],
        source_collection_paths=["trials/t/subtrials/s/images/p/plot/doc-1"],
        image_prefixes=image_prefixes or ["gs://bucket/images/sub/"],
    )
