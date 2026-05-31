from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
import os
from pathlib import PurePosixPath
from typing import Any

from firebase_admin import firestore

from cron_job.services.csv.assembler import _meta_image_uuid
from cron_job.services.gcs import download_text, parse_gcs_uri
from cron_job.schemas.models import InferenceJobResult, ScannedDocument


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "selected"}


def _batch_set(db, payloads: list[tuple[str, dict[str, Any]]]) -> int:
    count = 0
    for start in range(0, len(payloads), 400):
        batch = db.batch()
        for path, payload in payloads[start : start + 400]:
            batch.set(db.document(path), payload, merge=True)
            count += 1
        batch.commit()
    return count


def read_selected_image_rows(storage_client, selected_shard_uris: list[str]) -> dict[str, dict[str, Any]]:
    rows_by_image_id: dict[str, dict[str, Any]] = {}
    for uri in selected_shard_uris:
        try:
            text = download_text(storage_client, uri)
        except Exception:
            continue
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            image_id = str(row.get("meta_image_uuid") or "").strip()
            if not image_id:
                image_path = str(row.get("img_gcs_img_path") or "").strip()
                if image_path:
                    image_id = PurePosixPath(image_path).stem
            if not image_id:
                continue
            rows_by_image_id[image_id] = row
    return rows_by_image_id


def build_selected_image_prefixes(
    *,
    image_documents: list[ScannedDocument],
    selected_rows_by_image_id: dict[str, dict[str, Any]],
    selected_images_bucket: str,
) -> tuple[set[str], dict[str, str]]:
    selected_ids: set[str] = set()
    prefixes_by_doc_id: dict[str, str] = {}
    docs_by_image_id = {
        _meta_image_uuid(doc.fields, doc.document_id): doc
        for doc in image_documents
    }
    for image_id, row in selected_rows_by_image_id.items():
        if not _truthy(row.get("selected")):
            continue
        doc = docs_by_image_id.get(image_id)
        if not doc:
            continue
        selected_ids.add(image_id)
        gcs_path = str(row.get("img_gcs_img_path") or doc.fields.get("gcs_img_path") or "").strip()
        if not gcs_path:
            continue
        if gcs_path.startswith("gs://"):
            try:
                source_bucket, source_path = parse_gcs_uri(gcs_path)
                gcs_path = source_path
            except Exception:
                source_bucket = ""
        else:
            source_bucket = ""
        copied = _truthy(row.get("copied"))
        bucket = selected_images_bucket if copied else str(row.get("img_bucket_prefix") or doc.fields.get("bucket_prefix") or "").strip()
        if bucket.startswith("gs://"):
            bucket = bucket[5:].strip("/")
        if not bucket and source_bucket:
            bucket = source_bucket
        if not bucket:
            try:
                bucket, gcs_path = parse_gcs_uri(doc.image_uri or "")
            except Exception:
                continue
        parent = str(PurePosixPath(gcs_path).parent).strip(".")
        prefixes_by_doc_id[doc.document_id] = f"gs://{bucket}/{parent.rstrip('/')}/"
    return selected_ids, prefixes_by_doc_id


def write_back_preprocessing_status(
    db,
    *,
    image_documents: list[ScannedDocument],
    selected_image_ids: set[str],
    preprocessing_success: bool,
    batch_run_id: str,
    run_id: str,
    logger,
    trial_id: str,
    subtrial_id: str,
) -> int:
    timestamp = datetime.now(timezone.utc).isoformat()
    payloads: list[tuple[str, dict[str, Any]]] = []
    for doc in image_documents:
        image_id = _meta_image_uuid(doc.fields, doc.document_id)
        status = "skipped"
        if preprocessing_success:
            status = "selected" if image_id in selected_image_ids else "rejected"
        payload: dict[str, Any] = {
            "preprocessing_status": status,
            "preprocessing_timestamp": timestamp,
        }
        if preprocessing_success:
            payload.update(
                {
                    "preprocessing_batch_run_id": batch_run_id,
                    "preprocessing_run_id": run_id,
                }
            )
        payloads.append((doc.collection_path, payload))

    try:
        count = _batch_set(db, payloads)
        logger.log(
            "firestore_writeback",
            "success",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            write_type="preprocessing",
            document_count=count,
        )
        return count
    except Exception as exc:
        logger.log(
            "firestore_writeback",
            "failed",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            write_type="preprocessing",
            exc=exc,
        )
        return 0


def _normalize_path(path: str) -> list[str]:
    if path.startswith("gs://"):
        path = path[5:]
    path = path.rstrip("/")
    return [part for part in path.split("/") if part]


def _denormalize_path(parts: list[str], is_gs: bool) -> str:
    if is_gs:
        return "gs://" + "/".join(parts)
    return "/".join(parts)


def _compute_output_path(image_uri: str, source_prefix: str, save_folder: str) -> str:
    is_gs = save_folder.startswith("gs://")
    image_parts = _normalize_path(image_uri)
    prefix_parts = _normalize_path(source_prefix)
    save_parts = _normalize_path(save_folder)

    common_len = 0
    for prefix_part, save_part in zip(prefix_parts, save_parts):
        if prefix_part != save_part:
            break
        common_len += 1

    if len(image_parts) < len(prefix_parts):
        name, _ = os.path.splitext(PurePosixPath(image_uri).name)
        return f"{save_folder.rstrip('/')}/{name}.json"

    prefix_matches = all(
        index < len(image_parts) and image_parts[index] == prefix_parts[index]
        for index in range(len(prefix_parts))
    )
    if not prefix_matches:
        name, _ = os.path.splitext(PurePosixPath(image_uri).name)
        return f"{save_folder.rstrip('/')}/{name}.json"

    relative_parts = image_parts[len(prefix_parts):]
    output_parts = save_parts[:]

    if common_len < len(prefix_parts):
        output_parts.extend(prefix_parts[common_len + 1:])

    if relative_parts:
        output_parts.extend(relative_parts[:-1])
        name, _ = os.path.splitext(relative_parts[-1])
        output_parts.append(f"{name}.json")
    else:
        name, _ = os.path.splitext(PurePosixPath(image_uri).name)
        output_parts.append(f"{name}.json")
    return _denormalize_path(output_parts, is_gs)


def _source_image_uri_and_prefix(doc: ScannedDocument, prefixes: list[str]) -> tuple[str, str]:
    image_path = str(doc.fields.get("gcs_img_path") or doc.image_uri or doc.document_id)
    image_name = PurePosixPath(image_path).name
    image_blob_path = image_path[5:].partition("/")[2] if image_path.startswith("gs://") else image_path
    image_parent = str(PurePosixPath(image_blob_path).parent).strip(".").rstrip("/")

    for prefix in prefixes:
        prefix = prefix.rstrip("/") + "/"
        if doc.image_uri and doc.image_uri.startswith(prefix):
            return doc.image_uri, prefix

        prefix_blob_path = prefix[5:].partition("/")[2] if prefix.startswith("gs://") else prefix
        prefix_blob_path = prefix_blob_path.rstrip("/")
        if image_parent and prefix_blob_path == image_parent and image_name:
            return f"{prefix}{image_name}", prefix

    for prefix in prefixes:
        prefix = prefix.rstrip("/") + "/"
        if image_name:
            return f"{prefix}{image_name}", prefix

    if doc.image_uri:
        source_prefix = doc.image_uri.rsplit("/", 1)[0].rstrip("/") + "/"
        return doc.image_uri, source_prefix
    source_prefix = image_path.rsplit("/", 1)[0].rstrip("/") + "/"
    return image_path, source_prefix


def _compute_annotated_path(image_uri: str, source_prefix: str, save_folder: str) -> str:
    temp_json_path = _compute_output_path(image_uri, source_prefix, save_folder)
    dir_part = temp_json_path.rsplit("/", 1)[0] if "/" in temp_json_path else ""
    source_name = PurePosixPath(image_uri).name
    name, ext = os.path.splitext(source_name)
    ext = ext or ".jpg"
    return f"{dir_part}/annotated_{name}{ext}" if dir_part else f"annotated_{name}{ext}"


def write_back_inference_results(
    db,
    *,
    image_documents: list[ScannedDocument],
    inference_results: list[InferenceJobResult],
    run_id: str,
    logger,
    trial_id: str,
    subtrial_id: str,
) -> int:
    docs_by_path = {doc.collection_path: doc for doc in image_documents}
    payloads: list[tuple[str, dict[str, Any]]] = []
    for result in inference_results:
        if not result.success or not result.job_id or not result.output_gcs_path:
            continue
        for collection_path in result.source_collection_paths:
            doc = docs_by_path.get(collection_path)
            if not doc:
                continue
            image_uri, source_prefix = _source_image_uri_and_prefix(doc, result.image_prefixes or [])
            payloads.append(
                (
                    collection_path,
                    {
                        "inference_job_id": result.job_id,
                        "inference_output_json_path": _compute_output_path(
                            image_uri,
                            source_prefix,
                            result.output_gcs_path,
                        ),
                        "annotated_img_path": _compute_annotated_path(
                            image_uri,
                            source_prefix,
                            result.annotated_output_gcs_path or "",
                        )
                        if result.annotated_output_gcs_path
                        else "",
                        "inference_run_id": run_id,
                    },
                )
            )

    if not payloads:
        return 0
    try:
        count = _batch_set(db, payloads)
        logger.log(
            "firestore_writeback",
            "success",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            write_type="inference",
            document_count=count,
        )
        return count
    except Exception as exc:
        logger.log(
            "firestore_writeback",
            "failed",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            write_type="inference",
            exc=exc,
        )
        return 0
