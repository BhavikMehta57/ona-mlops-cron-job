from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal


CanonicalTrait = Literal["pods", "flowering", "plantstand"]
PipelineMethod = Literal["computer_vision", "classical"]
PathStatus = Literal["not_applicable", "succeeded", "failed", "completed_with_errors", "skipped"]


@dataclass(frozen=True)
class RunContext:
    run_id: str
    utc_date: date
    started_at: datetime


@dataclass(frozen=True)
class SubtrialInfo:
    trial_id: str
    subtrial_id: str
    trial_layout_id: str
    trial_data: dict[str, Any]
    subtrial_data: dict[str, Any]
    layout_data: dict[str, Any]


@dataclass(frozen=True)
class ScannedDocument:
    collection_name: str
    document_id: str
    collection_path: str
    protocol_date_id: str | None
    image_uri: str | None
    protocol: str | None
    trait: str | None
    upload_timestamp: datetime
    fields: dict[str, Any]


@dataclass
class SubtrialDocuments:
    image_documents: list[ScannedDocument] = field(default_factory=list)
    classical_documents: list[ScannedDocument] = field(default_factory=list)
    scan_errors: list[str] = field(default_factory=list)
    skipped_two_images_count: int = 0


@dataclass(frozen=True)
class ProtocolTraitGroup:
    protocol: str
    raw_trait_value: str
    canonical_trait_name: CanonicalTrait
    inference_trait_type: str
    source_document_ids: list[str]
    source_collection_paths: list[str]
    image_prefixes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PreprocessorResult:
    success: bool
    batch_run_id: str
    status: str
    selected_shard_uris: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass(frozen=True)
class InferenceJobResult:
    protocol: str
    raw_trait_value: str
    canonical_trait_name: CanonicalTrait
    inference_trait_type: str
    success: bool
    job_id: str | None
    output_gcs_path: str | None
    annotated_output_gcs_path: str | None = None
    source_document_ids: list[str] = field(default_factory=list)
    source_collection_paths: list[str] = field(default_factory=list)
    image_prefixes: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass(frozen=True)
class ExtractionRunResult:
    method: PipelineMethod
    canonical_trait_name: CanonicalTrait
    success: bool
    run_id: str | None
    output_prefix: str | None
    status: str
    operation_name: str | None = None
    execution_name: str | None = None
    error: str | None = None


@dataclass
class SubtrialState:
    trial_id: str
    subtrial_id: str
    index: int
    batch_run_id: str
    status: Literal["pending", "succeeded", "failed", "skipped"] = "pending"
    failed_stage: str | None = None
    image_document_count: int = 0
    classical_document_count: int = 0
    images_csv_uri: str | None = None
    classical_csv_uri: str | None = None
    cv_path_status: PathStatus = "not_applicable"
    classical_path_status: PathStatus = "not_applicable"
    preprocessing_writeback_count: int = 0
    inference_writeback_count: int = 0
    warnings: list[str] = field(default_factory=list)
