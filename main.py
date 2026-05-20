
from __future__ import annotations

import argparse
import csv
import io
import re
import uuid
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import PurePosixPath
from typing import Iterable, Sequence

from cron_job.core.config import AppConfig, load_config
from cron_job.csv_assembler import _meta_image_uuid, upload_classical_csv, upload_csv
from cron_job.csv_writeback import write_back_csv
from cron_job.db.firestore import get_firestore_client
from cron_job.db.gstorage import get_storage_client
from cron_job.firestore_client import discover_active_subtrials, scan_subtrial_documents
from cron_job.firestore_writeback import (
    build_selected_image_prefixes,
    read_selected_image_rows,
    write_back_inference_results,
    write_back_preprocessing_status,
)
from cron_job.inference_client import run_inference
from cron_job.middleware.logger import get_logger
from cron_job.models import (
    CanonicalTrait,
    ExtractionRunResult,
    InferenceJobResult,
    PathStatus,
    RunContext,
    ScannedDocument,
    SubtrialDocuments,
    SubtrialInfo,
    SubtrialState,
)
from cron_job.preprocessor_client import run_preprocessing, selected_shards_from_firestore
from cron_job.protocol_trait_resolver import group_documents_by_protocol_trait, inference_trait_type
from cron_job.trait_extractor import CloudRunJobClient, run_classical_extractions, run_cv_extractions
from cron_job.gcs_client import download_text, list_blob_uris, parse_gcs_uri


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _generate_run_id() -> str:
    return f"daily-{_utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


LOCAL_RAW_PREFIX_ROOT = "raw/local-test"
LOCAL_INFERENCE_PREFIX_ROOT = "inference/local-test"
LOCAL_EXTRACTION_PREFIX_ROOT = "extraction/local-test"
STEP_CHOICES = (
    "discover",
    "scan",
    "csv",
    "preprocess",
    "inference",
    "extract-cv",
    "extract-classical",
    "csv-writeback",
    "full",
)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc


def _parse_dates(value: str) -> list[date]:
    """Parse a delimited list of dates (YYYY-MM-DD). Accepts + or ; as separators."""
    parts = [p.strip() for p in re.split(r"[+;]+", value) if p.strip()]
    dates: list[date] = []
    for part in parts:
        try:
            dates.append(date.fromisoformat(part))
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"each date must be YYYY-MM-DD, got: {part!r}") from exc
    if not dates:
        raise argparse.ArgumentTypeError("at least one date is required")
    return dates


def _slug(value: str, *, max_len: int = 80) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip("-")
    return (slug or "unknown")[:max_len]


def _batch_run_id(run_id: str, index: int, subtrial_id: str) -> str:
    return f"{run_id}-{index:03d}-{_slug(subtrial_id, max_len=4)}"


def _step_batch_run_id(args: argparse.Namespace, context: RunContext, subtrial_id: str | None = None) -> str:
    if args.batch_run_id:
        return args.batch_run_id
    return context.run_id


def _require(value: str | None, name: str) -> str:
    if value:
        return value
    raise ValueError(f"{name} is required for this step")


def _apply_local_step_prefixes(config: AppConfig, args: argparse.Namespace) -> AppConfig:
    if not args.step:
        return config
    return replace(
        config,
        raw_prefix_root=(args.raw_prefix_root or LOCAL_RAW_PREFIX_ROOT).strip("/") or LOCAL_RAW_PREFIX_ROOT,
        inference_prefix_root=LOCAL_INFERENCE_PREFIX_ROOT,
        extraction_prefix_root=LOCAL_EXTRACTION_PREFIX_ROOT,
    )


def _load_subtrial_info(db, trial_id: str, subtrial_id: str) -> SubtrialInfo:
    trial_ref = db.collection("trials").document(trial_id)
    trial_snap = trial_ref.get()
    if not trial_snap.exists:
        raise ValueError(f"Trial not found: trials/{trial_id}")
    subtrial_ref = trial_ref.collection("subtrials").document(subtrial_id)
    subtrial_snap = subtrial_ref.get()
    if not subtrial_snap.exists:
        raise ValueError(f"Subtrial not found: trials/{trial_id}/subtrials/{subtrial_id}")
    return SubtrialInfo(
        trial_id=trial_id,
        subtrial_id=subtrial_id,
        trial_layout_id="manual",
        trial_data=trial_snap.to_dict() or {},
        subtrial_data=subtrial_snap.to_dict() or {},
        layout_data={},
    )


def _select_subtrials(db, args: argparse.Namespace, logger) -> list[SubtrialInfo]:
    if args.trial_id or args.subtrial_id:
        trial_id = _require(args.trial_id, "--trial-id")
        subtrial_id = _require(args.subtrial_id, "--subtrial-id")
        return [_load_subtrial_info(db, trial_id, subtrial_id)]
    subtrials = discover_active_subtrials(db, logger)
    if args.limit_subtrials is not None:
        subtrials = subtrials[: args.limit_subtrials]
    return subtrials


def _scan_for_args(db, args: argparse.Namespace, context: RunContext, logger, *, data_collected_date: str | None = None) -> tuple[SubtrialInfo, SubtrialDocuments]:
    trial_id = _require(args.trial_id, "--trial-id")
    subtrial_id = _require(args.subtrial_id, "--subtrial-id")
    subtrial = _load_subtrial_info(db, trial_id, subtrial_id)
    dc_date = data_collected_date or (args.data_collected_date.isoformat() if getattr(args, "data_collected_date", None) else None)
    return subtrial, scan_subtrial_documents(db, subtrial, context.utc_date, logger, data_collected_date=dc_date)


def _csv_subtrial_id_from_uri(uri: str, suffix: str) -> str:
    _, blob_name = parse_gcs_uri(uri)
    name = PurePosixPath(blob_name).name
    if name.endswith(suffix):
        return name[: -len(suffix)]
    return PurePosixPath(name).stem


def _trial_id_from_csv_uri(uri: str) -> str:
    _, blob_name = parse_gcs_uri(uri)
    parts = PurePosixPath(blob_name).parts
    if len(parts) >= 2:
        return parts[-2]
    return "unknown"


def _docs_from_raw_csv_uri(storage_client, uri: str, csv_type: str) -> list[ScannedDocument]:
    text = download_text(storage_client, uri)
    reader = csv.DictReader(io.StringIO(text))
    docs: list[ScannedDocument] = []
    timestamp = _utc_now()
    for row in reader:
        fields = dict(row)
        collection_name = row.get("collection_name") or ("images" if csv_type == "images" else "classical")
        docs.append(
            ScannedDocument(
                collection_name=collection_name,
                document_id=row.get("document_id") or "",
                collection_path=row.get("collection_path") or row.get("firestore_path") or row.get("document_id") or "",
                protocol_date_id=row.get("protocol_date_id") or None,
                image_uri=row.get("image_uri") or None,
                protocol=row.get("protocol") or None,
                trait=row.get("trait") or None,
                upload_timestamp=timestamp,
                fields=fields,
            )
        )
    return docs


def _canonical_trait_from_path(value: str) -> CanonicalTrait | None:
    if value in {"pods", "flowering", "plantstand"}:
        return value  # type: ignore[return-value]
    return None


def _discover_inference_results_from_gcs(
    storage_client,
    config: AppConfig,
    *,
    run_id: str,
    subtrial_id: str,
) -> list[InferenceJobResult]:
    base_prefix = f"{config.inference_prefix_root}/{run_id}/{subtrial_id}/"
    uris = list_blob_uris(storage_client, config.gcs_bucket, base_prefix)
    seen: set[tuple[str, str, str]] = set()
    results: list[InferenceJobResult] = []
    for uri in uris:
        if not uri.lower().endswith(".json") or "/json/" not in uri:
            continue
        _, blob_name = parse_gcs_uri(uri)
        rel = blob_name[len(base_prefix):] if blob_name.startswith(base_prefix) else blob_name
        parts = PurePosixPath(rel).parts
        if len(parts) < 4 or parts[2] != "json":
            continue
        canonical = _canonical_trait_from_path(parts[0])
        if canonical is None:
            continue
        protocol = parts[1]
        output_prefix = f"gs://{config.gcs_bucket}/{base_prefix}{parts[0]}/{protocol}/json/"
        key = (canonical, protocol, output_prefix)
        if key in seen:
            continue
        seen.add(key)
        annotated_prefix = f"gs://{config.gcs_bucket}/{base_prefix}{parts[0]}/{protocol}/annotated/"
        results.append(
            InferenceJobResult(
                protocol=protocol,
                raw_trait_value=canonical,
                canonical_trait_name=canonical,
                inference_trait_type=inference_trait_type(canonical),
                success=True,
                job_id="local-discovered",
                output_gcs_path=output_prefix,
                annotated_output_gcs_path=annotated_prefix,
            )
        )
    return results


def _discover_extraction_results_from_gcs(
    storage_client,
    config: AppConfig,
    *,
    run_id: str,
    subtrial_id: str,
    method: str,
) -> list[ExtractionRunResult]:
    base_prefix = f"{config.extraction_prefix_root}/{run_id}/{subtrial_id}/"
    scan_prefix = f"{base_prefix}classical/" if method == "classical" else base_prefix
    uris = list_blob_uris(storage_client, config.gcs_bucket, scan_prefix)
    seen: set[tuple[str, str]] = set()
    results: list[ExtractionRunResult] = []
    for uri in uris:
        lower = uri.lower()
        if not lower.endswith(".csv") or ("plot-level" not in lower and "plot_level" not in lower):
            continue
        _, blob_name = parse_gcs_uri(uri)
        rel = blob_name[len(scan_prefix):] if blob_name.startswith(scan_prefix) else blob_name
        parts = PurePosixPath(rel).parts
        if len(parts) < 3:
            continue
        canonical = _canonical_trait_from_path(parts[0])
        if canonical is None:
            continue
        prefix = uri.rsplit("/", 1)[0].rstrip("/") + "/"
        key = (canonical, prefix)
        if key in seen:
            continue
        seen.add(key)
        results.append(
            ExtractionRunResult(
                method="classical" if method == "classical" else "computer_vision",
                canonical_trait_name=canonical,
                success=True,
                run_id="local-discovered",
                output_prefix=prefix,
                status="completed",
            )
        )
    return results


def _extract_planting_date(subtrial: SubtrialInfo) -> str | None:
    keys = ("planting_date", "plantingDate", "planted_at", "plantedAt")
    for source in (subtrial.subtrial_data, subtrial.layout_data, subtrial.trial_data):
        for key in keys:
            value = source.get(key)
            if value is None or value == "":
                continue
            if isinstance(value, datetime):
                return value.date().isoformat()
            if isinstance(value, date):
                return value.isoformat()
            return str(value)
    return None


def _path_status_from_results(
    *,
    expected_count: int,
    inference_results: Iterable[InferenceJobResult] = (),
    extraction_results: Iterable[ExtractionRunResult] = (),
    csv_writeback_success: bool = True,
) -> PathStatus:
    inference_results = list(inference_results)
    extraction_results = list(extraction_results)
    failures = any(not result.success for result in inference_results)
    failures = failures or any(not result.success for result in extraction_results)
    failures = failures or not csv_writeback_success

    if expected_count <= 0:
        return "skipped"
    if not inference_results and not extraction_results:
        return "failed"
    successes = any(result.success for result in inference_results) or any(
        result.success for result in extraction_results
    )
    if failures and successes:
        return "completed_with_errors"
    if failures:
        return "failed"
    return "succeeded"


def _state_failed(state: SubtrialState, docs: SubtrialDocuments) -> bool:
    if docs.scan_errors:
        state.failed_stage = state.failed_stage or "scanning"
        return True
    for status in (state.cv_path_status, state.classical_path_status):
        if status in {"failed", "completed_with_errors"}:
            return True
    return False


def _selected_image_docs(
    image_documents: list[ScannedDocument],
    selected_image_ids: set[str],
    prefixes_by_doc_id: dict[str, str],
    logger,
    *,
    trial_id: str,
    subtrial_id: str,
) -> list[ScannedDocument]:
    selected: list[ScannedDocument] = []
    missing_prefix_count = 0
    for doc in image_documents:
        image_id = _meta_image_uuid(doc.fields, doc.document_id)
        if image_id not in selected_image_ids:
            continue
        if doc.document_id not in prefixes_by_doc_id:
            missing_prefix_count += 1
            continue
        selected.append(doc)

    if missing_prefix_count:
        logger.log(
            "preprocessing",
            "warning",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            document_count=missing_prefix_count,
            errors="selected image rows were missing usable GCS prefixes",
        )
    return selected


def _process_cv_path(
    config: AppConfig,
    *,
    db,
    storage_client,
    cloud_run_client: CloudRunJobClient,
    context: RunContext,
    subtrial: SubtrialInfo,
    state: SubtrialState,
    docs: SubtrialDocuments,
    logger,
) -> PathStatus:
    if not docs.image_documents:
        return "not_applicable"

    # Split documents by protocol and upload separate CSVs
    docs_by_protocol: dict[str, list[ScannedDocument]] = {}
    for doc in docs.image_documents:
        protocol = doc.protocol or "unknown"
        docs_by_protocol.setdefault(protocol, []).append(doc)

    uploaded_uris: list[str] = []
    for protocol, protocol_docs in docs_by_protocol.items():
        protocol_slug = _slug(protocol, max_len=60)
        uri = upload_csv(
            storage_client,
            bucket_name=config.gcs_bucket,
            run_id=context.run_id,
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            csv_type=f"images_{protocol_slug}",
            documents=protocol_docs,
            logger=logger,
            raw_prefix_root=config.raw_prefix_root,
        )
        if uri:
            uploaded_uris.append(uri)

    if not uploaded_uris:
        state.failed_stage = "csv_upload"
        return "failed"

    state.images_csv_uri = uploaded_uris[0]

    preprocessing = run_preprocessing(
        config,
        db=db,
        image_csv_uri=uploaded_uris[0],
        batch_run_id=state.batch_run_id,
        logger=logger,
    )
    selected_rows = (
        read_selected_image_rows(storage_client, preprocessing.selected_shard_uris)
        if preprocessing.success
        else {}
    )
    selected_ids, prefixes_by_doc_id = build_selected_image_prefixes(
        image_documents=docs.image_documents,
        selected_rows_by_image_id=selected_rows,
        selected_images_bucket=config.selected_images_bucket,
    )
    state.preprocessing_writeback_count = write_back_preprocessing_status(
        db,
        image_documents=docs.image_documents,
        selected_image_ids=selected_ids,
        preprocessing_success=preprocessing.success,
        batch_run_id=preprocessing.batch_run_id,
        run_id=context.run_id,
        logger=logger,
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
    )
    if not preprocessing.success:
        state.failed_stage = "preprocessing"
        return "failed"

    selected_docs = _selected_image_docs(
        docs.image_documents,
        selected_ids,
        prefixes_by_doc_id,
        logger,
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
    )
    if not selected_docs:
        logger.log(
            "cv_path",
            "skipped",
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            document_count=len(docs.image_documents),
            selected_document_count=0,
        )
        return "skipped"

    groups = group_documents_by_protocol_trait(
        selected_docs,
        logger,
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        image_prefixes_by_document_id=prefixes_by_doc_id,
    )
    groups = [group for group in groups if group.image_prefixes]
    if not groups:
        state.failed_stage = "protocol_trait_resolution"
        logger.log(
            "cv_path",
            "failed",
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            document_count=len(selected_docs),
            errors="no valid protocol/trait image groups after preprocessing",
        )
        return "failed"

    inference_results = run_inference(
        config,
        run_id=context.run_id,
        subtrial_id=subtrial.subtrial_id,
        groups=groups,
        logger=logger,
    )
    state.inference_writeback_count = write_back_inference_results(
        db,
        image_documents=selected_docs,
        inference_results=inference_results,
        run_id=context.run_id,
        logger=logger,
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
    )
    successful_inference_count = sum(1 for result in inference_results if result.success)
    if successful_inference_count == 0:
        state.failed_stage = "inference"
        return "failed"

    extraction_results = run_cv_extractions(
        config,
        db=db,
        cloud_run_client=cloud_run_client,
        run_id=context.run_id,
        subtrial_id=subtrial.subtrial_id,
        inference_results=inference_results,
        logger=logger,
        planting_date=_extract_planting_date(subtrial),
    )
    if not extraction_results:
        state.failed_stage = state.failed_stage or "trait_extraction"
        return "failed"
    csv_ok = True
    status = _path_status_from_results(
        expected_count=len(groups),
        inference_results=inference_results,
        extraction_results=extraction_results,
        csv_writeback_success=csv_ok,
    )
    if status in {"failed", "completed_with_errors"}:
        state.failed_stage = state.failed_stage or "computer_vision"
    return status


def _process_classical_path(
    config: AppConfig,
    *,
    db,
    storage_client,
    cloud_run_client: CloudRunJobClient,
    context: RunContext,
    subtrial: SubtrialInfo,
    state: SubtrialState,
    docs: SubtrialDocuments,
    logger,
) -> PathStatus:
    if not docs.classical_documents:
        return "not_applicable"

    groups = group_documents_by_protocol_trait(
        docs.classical_documents,
        logger,
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
    )
    if not groups:
        state.failed_stage = state.failed_stage or "protocol_trait_resolution"
        logger.log(
            "classical_path",
            "failed",
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            document_count=len(docs.classical_documents),
            errors="no valid protocol/trait classical groups",
        )
        return "failed"

    planting_date = _extract_planting_date(subtrial)
    if any(group.canonical_trait_name == "flowering" for group in groups) and not planting_date:
        logger.log(
            "classical_path",
            "warning",
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            errors="flowering classical extraction has no planting_date in trial/subtrial/layout data",
        )

    # Upload a separate CSV per trait group and run extraction for each
    docs_by_id = {doc.document_id: doc for doc in docs.classical_documents}
    extraction_results: list[ExtractionRunResult] = []
    for group in groups:
        group_docs = [docs_by_id[did] for did in group.source_document_ids if did in docs_by_id]
        if not group_docs:
            continue
        group_csv_uri = upload_classical_csv(
            storage_client,
            bucket_name=config.selected_images_bucket,
            run_id=context.run_id,
            run_date=str(context.utc_date),
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            documents=group_docs,
            logger=logger,
        )
        if not group_csv_uri:
            logger.log(
                "classical_path",
                "warning",
                trial_id=subtrial.trial_id,
                subtrial_id=subtrial.subtrial_id,
                trait=group.canonical_trait_name,
                errors="csv upload failed for trait group",
            )
            continue
        # Store the first uploaded URI for state tracking
        if not state.classical_csv_uri:
            state.classical_csv_uri = group_csv_uri
        group_results = run_classical_extractions(
            config,
            db=db,
            cloud_run_client=cloud_run_client,
            run_id=context.run_id,
            subtrial_id=subtrial.subtrial_id,
            input_csv=group_csv_uri,
            groups=[group],
            planting_date=planting_date,
            logger=logger,
        )
        extraction_results.extend(group_results)

    if not extraction_results:
        state.failed_stage = state.failed_stage or "csv_upload"
        return "failed"

    status = _path_status_from_results(
        expected_count=len(groups),
        extraction_results=extraction_results,
        csv_writeback_success=True,
    )
    if status in {"failed", "completed_with_errors"}:
        state.failed_stage = state.failed_stage or "classical"
    return status


def process_subtrial(
    config: AppConfig,
    *,
    db,
    storage_client,
    cloud_run_client: CloudRunJobClient,
    context: RunContext,
    subtrial: SubtrialInfo,
    index: int,
    logger,
    data_collected_date: str | None = None,
) -> SubtrialState:
    state = SubtrialState(
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        index=index,
        batch_run_id=_batch_run_id(context.run_id, index, subtrial.subtrial_id),
    )
    logger.log(
        "subtrial",
        "started",
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        subtrial_index=index,
        batch_run_id=state.batch_run_id,
    )

    try:
        docs = scan_subtrial_documents(db, subtrial, context.utc_date, logger, data_collected_date=data_collected_date)
        state.image_document_count = len(docs.image_documents)
        state.classical_document_count = len(docs.classical_documents)

        if state.image_document_count + state.classical_document_count == 0:
            state.status = "failed" if docs.scan_errors else "skipped"
            state.failed_stage = "scanning" if docs.scan_errors else None
            logger.log(
                "subtrial",
                state.status,
                trial_id=subtrial.trial_id,
                subtrial_id=subtrial.subtrial_id,
                subtrial_index=index,
                image_document_count=state.image_document_count,
                classical_document_count=state.classical_document_count,
                scan_error_count=len(docs.scan_errors),
            )
            return state

        state.cv_path_status = _process_cv_path(
            config,
            db=db,
            storage_client=storage_client,
            cloud_run_client=cloud_run_client,
            context=context,
            subtrial=subtrial,
            state=state,
            docs=docs,
            logger=logger,
        )
        state.classical_path_status = _process_classical_path(
            config,
            db=db,
            storage_client=storage_client,
            cloud_run_client=cloud_run_client,
            context=context,
            subtrial=subtrial,
            state=state,
            docs=docs,
            logger=logger,
        )
        state.status = "failed" if _state_failed(state, docs) else "succeeded"
        logger.log(
            "subtrial",
            state.status,
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            subtrial_index=index,
            image_document_count=state.image_document_count,
            classical_document_count=state.classical_document_count,
            cv_path_status=state.cv_path_status,
            classical_path_status=state.classical_path_status,
            failed_stage=state.failed_stage,
            preprocessing_writeback_count=state.preprocessing_writeback_count,
            inference_writeback_count=state.inference_writeback_count,
        )
        return state
    except Exception as exc:
        state.status = "failed"
        state.failed_stage = state.failed_stage or "unhandled_exception"
        logger.log_exception(
            "subtrial",
            "failed",
            exc,
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            subtrial_index=index,
            failed_stage=state.failed_stage,
        )
        return state


def _configure_logger(config: AppConfig, run_id: str, stdout_logger):
    if not config.use_cloud_logging:
        return stdout_logger
    try:
        from google.cloud import logging as cloud_logging
        from google.oauth2 import service_account

        credentials = None
        if config.gcp_service_account_credentials:
            credentials = service_account.Credentials.from_service_account_info(
                config.gcp_service_account_credentials
            )
        client = cloud_logging.Client(project=config.gcp_project_id, credentials=credentials)
        return get_logger(run_id, cloud_logger=client.logger("daily-pipeline-cron-job"))
    except Exception as exc:
        stdout_logger.log("bootstrap", "warning", exc=exc, errors="cloud logging unavailable; using stdout")
        return stdout_logger


def _smoke_test(config: AppConfig, logger) -> int:
    db = get_firestore_client(config)
    storage_client = get_storage_client(config)
    _ = db.collection("trial_layouts")
    _ = storage_client.bucket(config.gcs_bucket)
    _ = CloudRunJobClient(config)
    logger.log(
        "smoke_test",
        "success",
        gcs_bucket=config.gcs_bucket,
        firestore_database_id=config.firestore_database_id,
        gcp_project_id=config.gcp_project_id,
        gcp_run_region=config.gcp_run_region,
        trait_extraction_job_id=config.trait_extraction_job_id,
    )
    return 0


def _run_discover_step(db, args: argparse.Namespace, logger) -> int:
    subtrials = _select_subtrials(db, args, logger)
    logger.log(
        "local_step",
        "success",
        step="discover",
        discovered=len(subtrials),
        subtrials=[
            {"trial_id": subtrial.trial_id, "subtrial_id": subtrial.subtrial_id}
            for subtrial in subtrials
        ],
    )
    return 0


def _run_scan_step(db, args: argparse.Namespace, context: RunContext, logger) -> int:
    subtrial, docs = _scan_for_args(db, args, context, logger)
    logger.log(
        "local_step",
        "success" if not docs.scan_errors else "failed",
        step="scan",
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        image_document_count=len(docs.image_documents),
        classical_document_count=len(docs.classical_documents),
        skipped_two_images_count=docs.skipped_two_images_count,
        scan_error_count=len(docs.scan_errors),
    )
    return 1 if docs.scan_errors else 0


def _run_csv_step(
    config: AppConfig,
    *,
    db,
    storage_client,
    args: argparse.Namespace,
    context: RunContext,
    logger,
) -> int:
    subtrial, docs = _scan_for_args(db, args, context, logger)
    image_csv_uri = upload_csv(
        storage_client,
        bucket_name=config.gcs_bucket,
        run_id=context.run_id,
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        csv_type="images",
        documents=docs.image_documents,
        logger=logger,
        raw_prefix_root=config.raw_prefix_root,
    )
    classical_csv_uri = upload_classical_csv(
        storage_client,
        bucket_name=config.selected_images_bucket,
        run_id=context.run_id,
        run_date=str(context.utc_date),
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        documents=docs.classical_documents,
        logger=logger,
    )
    success = bool(image_csv_uri or classical_csv_uri) and not docs.scan_errors
    logger.log(
        "local_step",
        "success" if success else "failed",
        step="csv",
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        images_csv_uri=image_csv_uri,
        classical_csv_uri=classical_csv_uri,
        image_document_count=len(docs.image_documents),
        classical_document_count=len(docs.classical_documents),
    )
    return 0 if success else 1


def _run_preprocess_step(
    config: AppConfig,
    *,
    db,
    storage_client,
    args: argparse.Namespace,
    context: RunContext,
    logger,
) -> int:
    image_csv_uri = _require(args.image_csv_uri, "--image-csv-uri")
    batch_run_id = _step_batch_run_id(args, context, args.subtrial_id)
    result = run_preprocessing(
        config,
        db=db,
        image_csv_uri=image_csv_uri,
        batch_run_id=batch_run_id,
        logger=logger,
    )

    writeback_count = 0
    if result.success and args.trial_id and args.subtrial_id:
        subtrial, docs = _scan_for_args(db, args, context, logger)
        selected_rows = read_selected_image_rows(storage_client, result.selected_shard_uris)
        selected_ids, _ = build_selected_image_prefixes(
            image_documents=docs.image_documents,
            selected_rows_by_image_id=selected_rows,
            selected_images_bucket=config.selected_images_bucket,
        )
        writeback_count = write_back_preprocessing_status(
            db,
            image_documents=docs.image_documents,
            selected_image_ids=selected_ids,
            preprocessing_success=True,
            batch_run_id=result.batch_run_id,
            run_id=context.run_id,
            logger=logger,
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
        )

    logger.log(
        "local_step",
        "success" if result.success else "failed",
        step="preprocess",
        batch_run_id=batch_run_id,
        image_csv_uri=image_csv_uri,
        selected_shard_count=len(result.selected_shard_uris),
        preprocessing_writeback_count=writeback_count,
        errors=result.error,
    )
    return 0 if result.success else 1


def _run_inference_step(
    config: AppConfig,
    *,
    db,
    storage_client,
    args: argparse.Namespace,
    context: RunContext,
    logger,
) -> int:
    subtrial, docs = _scan_for_args(db, args, context, logger)
    batch_run_id = _step_batch_run_id(args, context, subtrial.subtrial_id)
    selected_shards = selected_shards_from_firestore(db, batch_run_id)
    if not selected_shards:
        logger.log(
            "local_step",
            "failed",
            step="inference",
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            batch_run_id=batch_run_id,
            errors="No selected preprocessing shard URIs found for batch_run_id",
        )
        return 1

    selected_rows = read_selected_image_rows(storage_client, selected_shards)
    selected_ids, prefixes_by_doc_id = build_selected_image_prefixes(
        image_documents=docs.image_documents,
        selected_rows_by_image_id=selected_rows,
        selected_images_bucket=config.selected_images_bucket,
    )
    write_back_preprocessing_status(
        db,
        image_documents=docs.image_documents,
        selected_image_ids=selected_ids,
        preprocessing_success=True,
        batch_run_id=batch_run_id,
        run_id=context.run_id,
        logger=logger,
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
    )
    selected_docs = _selected_image_docs(
        docs.image_documents,
        selected_ids,
        prefixes_by_doc_id,
        logger,
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
    )
    groups = group_documents_by_protocol_trait(
        selected_docs,
        logger,
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        image_prefixes_by_document_id=prefixes_by_doc_id,
    )
    groups = [group for group in groups if group.image_prefixes]
    if not groups:
        logger.log(
            "local_step",
            "failed",
            step="inference",
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            batch_run_id=batch_run_id,
            selected_document_count=len(selected_docs),
            errors="No valid selected protocol/trait groups found",
        )
        return 1

    results = run_inference(
        config,
        run_id=context.run_id,
        subtrial_id=subtrial.subtrial_id,
        groups=groups,
        logger=logger,
    )
    writeback_count = write_back_inference_results(
        db,
        image_documents=selected_docs,
        inference_results=results,
        run_id=context.run_id,
        logger=logger,
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
    )
    success = bool(results) and all(result.success for result in results)
    logger.log(
        "local_step",
        "success" if success else "failed",
        step="inference",
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        batch_run_id=batch_run_id,
        group_count=len(groups),
        successful_job_count=sum(1 for result in results if result.success),
        inference_writeback_count=writeback_count,
        output_gcs_paths=[result.output_gcs_path for result in results if result.output_gcs_path],
    )
    return 0 if success else 1


def _run_extract_cv_step(
    config: AppConfig,
    *,
    db,
    storage_client,
    cloud_run_client: CloudRunJobClient,
    args: argparse.Namespace,
    context: RunContext,
    logger,
) -> int:
    subtrial_id = _require(args.subtrial_id, "--subtrial-id")

    if args.inference_output_dir:
        # Use explicitly provided inference output path
        # Trait is inferred from --trait arg or defaults to the first recognized trait
        trait_name = getattr(args, "trait", None) or "flowering"
        inference_results = [
            InferenceJobResult(
                protocol="cli-provided",
                raw_trait_value=trait_name,
                canonical_trait_name=trait_name,
                inference_trait_type=inference_trait_type(trait_name),
                success=True,
                job_id="cli-provided",
                output_gcs_path=args.inference_output_dir.rstrip("/") + "/",
            )
        ]
    else:
        inference_results = _discover_inference_results_from_gcs(
            storage_client,
            config,
            run_id=context.run_id,
            subtrial_id=subtrial_id,
        )

    if not inference_results:
        logger.log(
            "local_step",
            "failed",
            step="extract-cv",
            subtrial_id=subtrial_id,
            errors="No inference JSON outputs found under the local-test inference prefix",
        )
        return 1
    planting_date = None
    if args.trial_id and args.subtrial_id:
        planting_date = _extract_planting_date(_load_subtrial_info(db, args.trial_id, args.subtrial_id))
    extraction_results = run_cv_extractions(
        config,
        db=db,
        cloud_run_client=cloud_run_client,
        run_id=context.run_id,
        subtrial_id=subtrial_id,
        inference_results=inference_results,
        logger=logger,
        planting_date=planting_date,
    )
    success = bool(extraction_results) and all(result.success for result in extraction_results)
    logger.log(
        "local_step",
        "success" if success else "failed",
        step="extract-cv",
        subtrial_id=subtrial_id,
        discovered_inference_output_count=len(inference_results),
        extraction_count=len(extraction_results),
        output_prefixes=[result.output_prefix for result in extraction_results if result.output_prefix],
    )
    return 0 if success else 1


def _run_extract_classical_step(
    config: AppConfig,
    *,
    db,
    storage_client,
    cloud_run_client: CloudRunJobClient,
    args: argparse.Namespace,
    context: RunContext,
    logger,
) -> int:
    classical_csv_uri = _require(args.classical_csv_uri, "--classical-csv-uri")
    trial_id = args.trial_id or _trial_id_from_csv_uri(classical_csv_uri)
    subtrial_id = args.subtrial_id or _csv_subtrial_id_from_uri(classical_csv_uri, "_classical.csv")
    docs = _docs_from_raw_csv_uri(storage_client, classical_csv_uri, "classical")
    groups = group_documents_by_protocol_trait(docs, logger, trial_id=trial_id, subtrial_id=subtrial_id)
    if not groups:
        logger.log(
            "local_step",
            "failed",
            step="extract-classical",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            classical_csv_uri=classical_csv_uri,
            errors="No valid protocol/trait groups found in classical CSV",
        )
        return 1
    planting_date = None
    if args.trial_id and args.subtrial_id:
        planting_date = _extract_planting_date(_load_subtrial_info(db, args.trial_id, args.subtrial_id))
    extraction_results = run_classical_extractions(
        config,
        db=db,
        cloud_run_client=cloud_run_client,
        run_id=context.run_id,
        subtrial_id=subtrial_id,
        input_csv=classical_csv_uri,
        groups=groups,
        planting_date=planting_date,
        logger=logger,
    )
    success = bool(extraction_results) and all(result.success for result in extraction_results)
    logger.log(
        "local_step",
        "success" if success else "failed",
        step="extract-classical",
        trial_id=trial_id,
        subtrial_id=subtrial_id,
        classical_csv_uri=classical_csv_uri,
        group_count=len(groups),
        extraction_count=len(extraction_results),
        output_prefixes=[result.output_prefix for result in extraction_results if result.output_prefix],
    )
    return 0 if success else 1


def _run_csv_writeback_step(
    config: AppConfig,
    *,
    storage_client,
    args: argparse.Namespace,
    context: RunContext,
    logger,
) -> int:
    if not args.image_csv_uri and not args.classical_csv_uri:
        raise ValueError("--image-csv-uri or --classical-csv-uri is required for csv-writeback")

    success = True
    if args.image_csv_uri:
        trial_id = args.trial_id or _trial_id_from_csv_uri(args.image_csv_uri)
        subtrial_id = args.subtrial_id or _csv_subtrial_id_from_uri(args.image_csv_uri, "_images.csv")
        extraction_results = _discover_extraction_results_from_gcs(
            storage_client,
            config,
            run_id=context.run_id,
            subtrial_id=subtrial_id,
            method="computer_vision",
        )
        success = write_back_csv(
            storage_client,
            raw_csv_uri=args.image_csv_uri,
            extraction_results=extraction_results,
            csv_type="images",
            logger=logger,
            trial_id=trial_id,
            subtrial_id=subtrial_id,
        ) and success

    if args.classical_csv_uri:
        trial_id = args.trial_id or _trial_id_from_csv_uri(args.classical_csv_uri)
        subtrial_id = args.subtrial_id or _csv_subtrial_id_from_uri(args.classical_csv_uri, "_classical.csv")
        extraction_results = _discover_extraction_results_from_gcs(
            storage_client,
            config,
            run_id=context.run_id,
            subtrial_id=subtrial_id,
            method="classical",
        )
        success = write_back_csv(
            storage_client,
            raw_csv_uri=args.classical_csv_uri,
            extraction_results=extraction_results,
            csv_type="classical",
            logger=logger,
            trial_id=trial_id,
            subtrial_id=subtrial_id,
        ) and success

    logger.log("local_step", "success" if success else "failed", step="csv-writeback")
    return 0 if success else 1


def _run_step(
    config: AppConfig,
    *,
    db,
    storage_client,
    args: argparse.Namespace,
    context: RunContext,
    logger,
) -> int:
    if args.step == "discover":
        return _run_discover_step(db, args, logger)
    if args.step == "scan":
        return _run_scan_step(db, args, context, logger)
    if args.step == "csv":
        return _run_csv_step(config, db=db, storage_client=storage_client, args=args, context=context, logger=logger)
    if args.step == "preprocess":
        return _run_preprocess_step(config, db=db, storage_client=storage_client, args=args, context=context, logger=logger)
    if args.step == "inference":
        return _run_inference_step(config, db=db, storage_client=storage_client, args=args, context=context, logger=logger)
    if args.step == "extract-cv":
        return _run_extract_cv_step(
            config,
            db=db,
            storage_client=storage_client,
            cloud_run_client=CloudRunJobClient(config),
            args=args,
            context=context,
            logger=logger,
        )
    if args.step == "extract-classical":
        return _run_extract_classical_step(
            config,
            db=db,
            storage_client=storage_client,
            cloud_run_client=CloudRunJobClient(config),
            args=args,
            context=context,
            logger=logger,
        )
    if args.step == "csv-writeback":
        # csv-writeback disabled — the trait extraction service handles output location
        logger.log("local_step", "skipped", step="csv-writeback", errors="csv-writeback is disabled")
        return 0
    raise ValueError(f"Unsupported step: {args.step}")


def _summary(context: RunContext, states: list[SubtrialState], discovered_count: int) -> dict[str, int | str]:
    return {
        "run_id": context.run_id,
        "utc_date": context.utc_date.isoformat(),
        "discovered": discovered_count,
        "succeeded": sum(1 for state in states if state.status == "succeeded"),
        "skipped": sum(1 for state in states if state.status == "skipped"),
        "failed": sum(1 for state in states if state.status == "failed"),
        "image_document_count": sum(state.image_document_count for state in states),
        "classical_document_count": sum(state.classical_document_count for state in states),
        "preprocessing_writeback_count": sum(state.preprocessing_writeback_count for state in states),
        "inference_writeback_count": sum(state.inference_writeback_count for state in states),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the ONA daily pipeline Cloud Run Job orchestrator.")
    parser.add_argument("--smoke-test", action="store_true", help="Validate configuration and clients without processing data.")
    parser.add_argument("--run-date", type=_parse_date, default=_utc_now().date(), help="UTC upload date to process (YYYY-MM-DD).")
    parser.add_argument("--run-id", default=None, help="Optional run id override for replays and tests.")
    parser.add_argument(
        "--step",
        choices=STEP_CHOICES,
        default=None,
        help="Run a single local test step. Use 'full' for an isolated local-test full run.",
    )
    parser.add_argument("--trial-id", default=None, help="Trial id to process, e.g. trial--site.")
    parser.add_argument("--subtrial-id", default=None, help="Subtrial id to process, e.g. season--field--location.")
    parser.add_argument("--limit-subtrials", type=int, default=None, help="Limit discovered active subtrials for local/manual runs.")
    parser.add_argument("--image-csv-uri", default=None, help="Raw image CSV URI for step testing.")
    parser.add_argument("--classical-csv-uri", default=None, help="Raw classical CSV URI for step testing.")
    parser.add_argument(
        "--raw-prefix-root",
        default=None,
        help="Raw CSV prefix root for --step runs. Defaults to raw/local-test.",
    )
    parser.add_argument(
        "--batch-run-id",
        default=None,
        help="Preprocessor batch_run_id to reuse for inference step testing. Defaults to --run-id.",
    )
    parser.add_argument(
        "--inference-output-dir",
        default=None,
        help="Inference output GCS path (save_json_folder) for extract-cv step. Skips GCS discovery.",
    )
    parser.add_argument(
        "--trait",
        default=None,
        help="Trait name (flowering, pods, plantstand) for extract-cv when using --inference-output-dir.",
    )
    parser.add_argument(
        "--data-collected-date",
        type=_parse_date,
        default=None,
        help="Filter by data_collection date (YYYY-MM-DD) instead of upload_timestamp. Mutually exclusive with --run-date.",
    )
    parser.add_argument(
        "--data-collected-dates",
        type=_parse_dates,
        default=None,
        help="Comma-separated list of data_collection dates (YYYY-MM-DD). Processes scan/csv/preprocess/inference per-date, then runs trait extraction once for all dates combined.",
    )
    return parser.parse_args(argv)


def _process_multi_date(
    config: AppConfig,
    *,
    db,
    storage_client,
    cloud_run_client: CloudRunJobClient,
    context: RunContext,
    subtrial: SubtrialInfo,
    dates: list[date],
    logger,
) -> SubtrialState:
    """Process multiple dates: scan/csv/preprocess/inference per-date, then trait extraction once for all."""
    state = SubtrialState(
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        index=0,
        batch_run_id=_batch_run_id(context.run_id, 0, subtrial.subtrial_id),
    )
    logger.log(
        "multi_date",
        "started",
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        dates=[d.isoformat() for d in dates],
        date_count=len(dates),
    )

    all_inference_results: list[InferenceJobResult] = []
    all_classical_documents: list[ScannedDocument] = []
    per_date_errors: list[str] = []

    for date_index, dc_date in enumerate(dates):
        dc_date_str = dc_date.isoformat()
        logger.log(
            "multi_date_phase1",
            "started",
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            data_collected_date=dc_date_str,
            date_index=date_index + 1,
            date_total=len(dates),
        )

        try:
            docs = scan_subtrial_documents(
                db, subtrial, context.utc_date, logger, data_collected_date=dc_date_str
            )

            if not docs.image_documents and not docs.classical_documents:
                logger.log(
                    "multi_date_phase1",
                    "skipped" if not docs.scan_errors else "failed",
                    trial_id=subtrial.trial_id,
                    subtrial_id=subtrial.subtrial_id,
                    data_collected_date=dc_date_str,
                    image_document_count=0,
                    classical_document_count=0,
                    scan_error_count=len(docs.scan_errors),
                )
                if docs.scan_errors:
                    per_date_errors.append(f"{dc_date_str}: scan errors")
                continue

            # Collect classical documents for combined extraction later
            all_classical_documents.extend(docs.classical_documents)

            # --- CV path: scan → csv → preprocess → inference (per-date) ---
            if docs.image_documents:
                # Split documents by protocol and upload separate CSVs
                docs_by_protocol: dict[str, list[ScannedDocument]] = {}
                for doc in docs.image_documents:
                    protocol = doc.protocol or "unknown"
                    docs_by_protocol.setdefault(protocol, []).append(doc)

                uploaded_uris: list[str] = []
                for protocol, protocol_docs in docs_by_protocol.items():
                    protocol_slug = _slug(protocol, max_len=60)
                    uri = upload_csv(
                        storage_client,
                        bucket_name=config.gcs_bucket,
                        run_id=context.run_id,
                        trial_id=subtrial.trial_id,
                        subtrial_id=subtrial.subtrial_id,
                        csv_type=f"images_{protocol_slug}_{dc_date_str}",
                        documents=protocol_docs,
                        logger=logger,
                        raw_prefix_root=config.raw_prefix_root,
                    )
                    if uri:
                        uploaded_uris.append(uri)

                if not uploaded_uris:
                    per_date_errors.append(f"{dc_date_str}: csv upload failed")
                    logger.log(
                        "multi_date_phase1",
                        "failed",
                        trial_id=subtrial.trial_id,
                        subtrial_id=subtrial.subtrial_id,
                        data_collected_date=dc_date_str,
                        errors="csv upload failed",
                    )
                    continue

                date_batch_run_id = f"{context.run_id}-{date_index:03d}-{dc_date_str}"
                preprocessing = run_preprocessing(
                    config,
                    db=db,
                    image_csv_uri=uploaded_uris[0],
                    batch_run_id=date_batch_run_id,
                    logger=logger,
                )

                if not preprocessing.success:
                    per_date_errors.append(f"{dc_date_str}: preprocessing failed")
                    logger.log(
                        "multi_date_phase1",
                        "failed",
                        trial_id=subtrial.trial_id,
                        subtrial_id=subtrial.subtrial_id,
                        data_collected_date=dc_date_str,
                        errors="preprocessing failed",
                    )
                    continue

                selected_rows = read_selected_image_rows(storage_client, preprocessing.selected_shard_uris)
                selected_ids, prefixes_by_doc_id = build_selected_image_prefixes(
                    image_documents=docs.image_documents,
                    selected_rows_by_image_id=selected_rows,
                    selected_images_bucket=config.selected_images_bucket,
                )
                write_back_preprocessing_status(
                    db,
                    image_documents=docs.image_documents,
                    selected_image_ids=selected_ids,
                    preprocessing_success=True,
                    batch_run_id=preprocessing.batch_run_id,
                    run_id=context.run_id,
                    logger=logger,
                    trial_id=subtrial.trial_id,
                    subtrial_id=subtrial.subtrial_id,
                )

                selected_docs = _selected_image_docs(
                    docs.image_documents,
                    selected_ids,
                    prefixes_by_doc_id,
                    logger,
                    trial_id=subtrial.trial_id,
                    subtrial_id=subtrial.subtrial_id,
                )
                if not selected_docs:
                    logger.log(
                        "multi_date_phase1",
                        "skipped",
                        trial_id=subtrial.trial_id,
                        subtrial_id=subtrial.subtrial_id,
                        data_collected_date=dc_date_str,
                        errors="no selected images after preprocessing",
                    )
                    continue

                groups = group_documents_by_protocol_trait(
                    selected_docs,
                    logger,
                    trial_id=subtrial.trial_id,
                    subtrial_id=subtrial.subtrial_id,
                    image_prefixes_by_document_id=prefixes_by_doc_id,
                )
                groups = [group for group in groups if group.image_prefixes]
                if not groups:
                    per_date_errors.append(f"{dc_date_str}: no valid protocol/trait groups")
                    continue

                inference_results = run_inference(
                    config,
                    run_id=context.run_id,
                    subtrial_id=subtrial.subtrial_id,
                    groups=groups,
                    logger=logger,
                )
                write_back_inference_results(
                    db,
                    image_documents=selected_docs,
                    inference_results=inference_results,
                    run_id=context.run_id,
                    logger=logger,
                    trial_id=subtrial.trial_id,
                    subtrial_id=subtrial.subtrial_id,
                )

                successful = [r for r in inference_results if r.success]
                all_inference_results.extend(successful)

                logger.log(
                    "multi_date_phase1",
                    "success",
                    trial_id=subtrial.trial_id,
                    subtrial_id=subtrial.subtrial_id,
                    data_collected_date=dc_date_str,
                    image_document_count=len(docs.image_documents),
                    classical_document_count=len(docs.classical_documents),
                    inference_success_count=len(successful),
                )
            else:
                logger.log(
                    "multi_date_phase1",
                    "success",
                    trial_id=subtrial.trial_id,
                    subtrial_id=subtrial.subtrial_id,
                    data_collected_date=dc_date_str,
                    image_document_count=0,
                    classical_document_count=len(docs.classical_documents),
                )

        except Exception as exc:
            per_date_errors.append(f"{dc_date_str}: {exc}")
            logger.log_exception(
                "multi_date_phase1",
                "failed",
                exc,
                trial_id=subtrial.trial_id,
                subtrial_id=subtrial.subtrial_id,
                data_collected_date=dc_date_str,
            )
            continue

    # --- Phase 2: Combined trait extraction ---
    logger.log(
        "multi_date_phase2",
        "started",
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        total_inference_results=len(all_inference_results),
        total_classical_documents=len(all_classical_documents),
    )

    planting_date = _extract_planting_date(subtrial)

    # CV extraction: all inference results combined
    if all_inference_results:
        cv_extraction_results = run_cv_extractions(
            config,
            db=db,
            cloud_run_client=cloud_run_client,
            run_id=context.run_id,
            subtrial_id=subtrial.subtrial_id,
            inference_results=all_inference_results,
            logger=logger,
            planting_date=planting_date,
        )
        cv_success = bool(cv_extraction_results) and any(r.success for r in cv_extraction_results)
        state.cv_path_status = "succeeded" if cv_success else "failed"
        logger.log(
            "multi_date_phase2",
            "success" if cv_success else "failed",
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
            path="cv",
            extraction_count=len(cv_extraction_results),
            successful_count=sum(1 for r in cv_extraction_results if r.success),
        )
    else:
        state.cv_path_status = "not_applicable"

    # Classical extraction: one CSV per trait group for all dates combined
    if all_classical_documents:
        groups = group_documents_by_protocol_trait(
            all_classical_documents,
            logger,
            trial_id=subtrial.trial_id,
            subtrial_id=subtrial.subtrial_id,
        )
        if groups:
            docs_by_id = {doc.document_id: doc for doc in all_classical_documents}
            classical_extraction_results: list[ExtractionRunResult] = []
            for group in groups:
                group_docs = [docs_by_id[did] for did in group.source_document_ids if did in docs_by_id]
                if not group_docs:
                    continue
                group_csv_uri = upload_classical_csv(
                    storage_client,
                    bucket_name=config.selected_images_bucket,
                    run_id=context.run_id,
                    run_date=str(context.utc_date),
                    trial_id=subtrial.trial_id,
                    subtrial_id=subtrial.subtrial_id,
                    documents=group_docs,
                    logger=logger,
                )
                if not group_csv_uri:
                    logger.log(
                        "multi_date_phase2",
                        "warning",
                        trial_id=subtrial.trial_id,
                        subtrial_id=subtrial.subtrial_id,
                        trait=group.canonical_trait_name,
                        errors="csv upload failed for trait group",
                    )
                    continue
                group_results = run_classical_extractions(
                    config,
                    db=db,
                    cloud_run_client=cloud_run_client,
                    run_id=context.run_id,
                    subtrial_id=subtrial.subtrial_id,
                    input_csv=group_csv_uri,
                    groups=[group],
                    planting_date=planting_date,
                    logger=logger,
                )
                classical_extraction_results.extend(group_results)

            if classical_extraction_results:
                classical_success = any(r.success for r in classical_extraction_results)
                state.classical_path_status = "succeeded" if classical_success else "failed"
                logger.log(
                    "multi_date_phase2",
                    "success" if classical_success else "failed",
                    trial_id=subtrial.trial_id,
                    subtrial_id=subtrial.subtrial_id,
                    path="classical",
                    extraction_count=len(classical_extraction_results),
                    successful_count=sum(1 for r in classical_extraction_results if r.success),
                )
            else:
                state.classical_path_status = "failed"
                state.failed_stage = "csv_upload"
        else:
            state.classical_path_status = "failed"
            state.failed_stage = "protocol_trait_resolution"
    else:
        state.classical_path_status = "not_applicable"

    # Final status
    has_failures = bool(per_date_errors) or state.cv_path_status == "failed" or state.classical_path_status == "failed"
    has_successes = state.cv_path_status == "succeeded" or state.classical_path_status == "succeeded"
    if has_failures and has_successes:
        state.status = "succeeded"  # partial success — some dates may have failed but extraction worked
    elif has_successes:
        state.status = "succeeded"
    else:
        state.status = "failed"

    logger.log(
        "multi_date",
        state.status,
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        dates=[d.isoformat() for d in dates],
        per_date_errors=per_date_errors,
        cv_path_status=state.cv_path_status,
        classical_path_status=state.classical_path_status,
    )
    return state


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    # Determine effective date: --data-collected-date takes precedence
    data_collected_date: str | None = None
    if args.data_collected_date:
        data_collected_date = args.data_collected_date.isoformat()
        effective_date = args.data_collected_date
    else:
        effective_date = args.run_date

    run_id = args.run_id or _generate_run_id()
    context = RunContext(run_id=run_id, utc_date=effective_date, started_at=_utc_now())
    logger = get_logger(run_id)

    try:
        config = load_config(require_services=True)
        config = _apply_local_step_prefixes(config, args)
        logger = _configure_logger(config, run_id, logger)
        logger.log(
            "bootstrap",
            "started",
            utc_date=context.utc_date.isoformat(),
            smoke_test=args.smoke_test,
            step=args.step,
            raw_prefix_root=config.raw_prefix_root,
            inference_prefix_root=config.inference_prefix_root,
            extraction_prefix_root=config.extraction_prefix_root,
        )

        if args.smoke_test:
            return _smoke_test(config, logger)

        db = get_firestore_client(config)
        storage_client = get_storage_client(config)
        if args.step and args.step != "full":
            return _run_step(
                config,
                db=db,
                storage_client=storage_client,
                args=args,
                context=context,
                logger=logger,
            )

        cloud_run_client = CloudRunJobClient(config)

        # --- Multi-date mode ---
        if args.data_collected_dates:
            trial_id = _require(args.trial_id, "--trial-id")
            subtrial_id = _require(args.subtrial_id, "--subtrial-id")
            subtrial = _load_subtrial_info(db, trial_id, subtrial_id)
            state = _process_multi_date(
                config,
                db=db,
                storage_client=storage_client,
                cloud_run_client=cloud_run_client,
                context=context,
                subtrial=subtrial,
                dates=args.data_collected_dates,
                logger=logger,
            )
            failed = state.status == "failed"
            logger.log("summary", "failed" if failed else "succeeded", run_id=context.run_id, subtrial_status=state.status)
            return 1 if failed else 0

        # --- Single-date / discovery mode ---
        subtrials = _select_subtrials(db, args, logger)
        states: list[SubtrialState] = []
        for index, subtrial in enumerate(subtrials, start=1):
            states.append(
                process_subtrial(
                    config,
                    db=db,
                    storage_client=storage_client,
                    cloud_run_client=cloud_run_client,
                    context=context,
                    subtrial=subtrial,
                    index=index,
                    logger=logger,
                    data_collected_date=data_collected_date,
                )
            )

        summary = _summary(context, states, len(subtrials))
        failed = int(summary["failed"])
        logger.log("summary", "failed" if failed else "succeeded", **summary)
        return 1 if failed else 0
    except Exception as exc:
        logger.log_exception("bootstrap", "fatal", exc, utc_date=context.utc_date.isoformat())
        return 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
