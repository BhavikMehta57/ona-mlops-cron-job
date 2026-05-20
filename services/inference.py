from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

import httpx

from cron_job.core.config import AppConfig
from cron_job.services.gcs import make_gcs_uri
from cron_job.schemas.models import InferenceJobResult, ProtocolTraitGroup


SUCCESS_STATUSES = {"completed", "complete", "success", "succeeded"}
FAILED_STATUSES = {"failed", "error", "cancelled", "canceled"}

# Mapping from inference_trait_type to (model_id, confidence)
TRAIT_MODEL_MAP: dict[str, tuple[str, float]] = {
    "flower": ("dup_merged_1_to_15_flower_inst_seg-mhpnh/8", 0.34),
    "pod": ("artemis2_pod_segmentation_batch3-gd8ng/12", 0.25),
    "plant_stand": ("bushbean_stand_count_diversity_gcp/4", 0.5),
    "crop_row": ("crop_row_detection-qwieb/3", 0.5),
}


def _backoff(attempt: int) -> int:
    return min(15 * (2 ** max(0, attempt - 1)), 120)


def _slug(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip())
    return value.strip("-") or "default"


def _model_path_part(model_id: str) -> str:
    """Convert model_id like 'workspace/version' to 'workspace-vversion'."""
    return model_id.strip().replace("/", "-v") or "{model_id}"


def _conf_path_part(confidence: float) -> str:
    """Convert confidence float to clean string: 0.2500 -> '0.25', 1.0 -> '1'."""
    s = str(confidence)
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _images_root(prefix: str) -> str:
    """Find the path up to and including /images/ in a GCS prefix."""
    p = prefix if prefix.endswith("/") else f"{prefix}/"
    marker = "/images/"
    idx = p.find(marker)
    if idx < 0:
        return ""
    return p[: idx + len(marker)]


def _shared_images_root(prefixes: list[str]) -> str:
    """Find the common /images/ root shared by all prefixes."""
    if not prefixes:
        return ""
    roots = [_images_root(p) for p in prefixes]
    if any(not r for r in roots):
        return ""
    return roots[0] if all(r == roots[0] for r in roots) else ""


def _compute_save_folder(prefixes: list[str], model_id: str, confidence: float) -> str:
    """Build save_json_folder the same way the dashboard does:
    shared_images_root.replace('/images/', '/images_inference/') + model_slug/confidence/
    """
    root = _shared_images_root(prefixes)
    if not root:
        return ""
    base = root.replace("/images/", "/images_inference/")
    model_part = _model_path_part(model_id)
    conf_part = _conf_path_part(confidence)
    return f"{base}{model_part}/{conf_part}/"


def _output_prefix(
    config: AppConfig,
    run_id: str,
    subtrial_id: str,
    trait: str,
    qualifier: str | None = None,
) -> str:
    """Fallback output prefix when images root cannot be determined."""
    suffix = f"{trait}/{_slug(qualifier)}" if qualifier else trait
    path = f"{config.inference_prefix_root}/{run_id}/{subtrial_id}/{suffix}/{kind}/"
    return make_gcs_uri(config.gcs_bucket, path)


def _submit_and_poll_one(
    config: AppConfig,
    *,
    run_id: str,
    subtrial_id: str,
    group: ProtocolTraitGroup,
    logger,
    sleep: Callable[[float], None] = time.sleep,
) -> InferenceJobResult:
    if not group.image_prefixes:
        error = "no selected image prefixes for group"
        logger.log(
            "inference",
            "failed",
            subtrial_id=subtrial_id,
            protocol=group.protocol,
            trait_type=group.inference_trait_type,
            errors=error,
        )
        return InferenceJobResult(
            group.protocol,
            group.raw_trait_value,
            group.canonical_trait_name,
            group.inference_trait_type,
            False,
            None,
            None,
            source_document_ids=group.source_document_ids,
            source_collection_paths=group.source_collection_paths,
            image_prefixes=group.image_prefixes,
            error=error,
        )

    model_id, model_confidence = TRAIT_MODEL_MAP.get(
        group.inference_trait_type, (None, config.inference_confidence)
    )
    if model_id is None:
        error = f"no model_id mapping for trait_type={group.inference_trait_type!r}"
        logger.log(
            "inference",
            "failed",
            subtrial_id=subtrial_id,
            protocol=group.protocol,
            trait_type=group.inference_trait_type,
            errors=error,
        )
        return InferenceJobResult(
            group.protocol,
            group.raw_trait_value,
            group.canonical_trait_name,
            group.inference_trait_type,
            False,
            None,
            None,
            source_document_ids=group.source_document_ids,
            source_collection_paths=group.source_collection_paths,
            image_prefixes=group.image_prefixes,
            error=error,
        )

    gcs_prefix = group.image_prefixes[0] if len(group.image_prefixes) == 1 else group.image_prefixes

    # Build output folder the same way the dashboard does:
    # shared /images/ root -> replace with /images_inference/ -> append model_slug/confidence/
    save_json_folder = _compute_save_folder(group.image_prefixes, model_id, model_confidence)
    if not save_json_folder:
        # Fallback if prefixes don't share an /images/ root
        save_json_folder = _output_prefix(
            config, run_id, subtrial_id, group.canonical_trait_name, group.protocol,
        )
    save_output_folder = save_json_folder

    payload = {
        "gcs_prefix": gcs_prefix,
        "model_id": model_id,
        "limit": config.inference_limit,
        "save_json_folder": save_json_folder,
        "save_output_folder": save_output_folder,
        "confidence": model_confidence,
        "trait_type": group.inference_trait_type,
        "async_processing": True,
        "queue": "ona-infer-queue-test",
        "location": "us-central1",
    }

    try:
        with httpx.Client(timeout=config.inference_request_timeout_s) as client:
            response = client.post(f"{config.inference_url}/batch", json=payload)
        if response.status_code >= 400:
            logger.log(
                "inference",
                "failed",
                subtrial_id=subtrial_id,
                protocol=group.protocol,
                trait_type=group.inference_trait_type,
                http_status=response.status_code,
                response_body=response.text,
            )
            return InferenceJobResult(
                group.protocol,
                group.raw_trait_value,
                group.canonical_trait_name,
                group.inference_trait_type,
                False,
                None,
                None,
                source_document_ids=group.source_document_ids,
                source_collection_paths=group.source_collection_paths,
                image_prefixes=group.image_prefixes,
                error=response.text,
            )
        body = response.json()
        job_id = body.get("job_id")
        if not job_id:
            error = "missing job_id in response"
            logger.log(
                "inference",
                "failed",
                subtrial_id=subtrial_id,
                protocol=group.protocol,
                trait_type=group.inference_trait_type,
                errors=error,
                response_body=body,
            )
            return InferenceJobResult(
                group.protocol,
                group.raw_trait_value,
                group.canonical_trait_name,
                group.inference_trait_type,
                False,
                None,
                None,
                source_document_ids=group.source_document_ids,
                source_collection_paths=group.source_collection_paths,
                image_prefixes=group.image_prefixes,
                error=error,
            )
    except Exception as exc:
        logger.log(
            "inference",
            "failed",
            subtrial_id=subtrial_id,
            protocol=group.protocol,
            trait_type=group.inference_trait_type,
            exc=exc,
        )
        return InferenceJobResult(
            group.protocol,
            group.raw_trait_value,
            group.canonical_trait_name,
            group.inference_trait_type,
            False,
            None,
            None,
            source_document_ids=group.source_document_ids,
            source_collection_paths=group.source_collection_paths,
            image_prefixes=group.image_prefixes,
            error=str(exc),
        )

    started = time.monotonic()
    attempt = 1
    while True:
        if time.monotonic() - started > config.inference_poll_timeout_s:
            logger.log(
                "inference",
                "timeout",
                subtrial_id=subtrial_id,
                protocol=group.protocol,
                trait_type=group.inference_trait_type,
                job_id=job_id,
            )
            return InferenceJobResult(
                group.protocol,
                group.raw_trait_value,
                group.canonical_trait_name,
                group.inference_trait_type,
                False,
                job_id,
                None,
                source_document_ids=group.source_document_ids,
                source_collection_paths=group.source_collection_paths,
                image_prefixes=group.image_prefixes,
                error="poll timeout",
            )

        sleep(_backoff(attempt))
        attempt += 1
        try:
            with httpx.Client(timeout=config.inference_request_timeout_s) as client:
                response = client.get(f"{config.inference_url}/batch-status/{job_id}")
            response.raise_for_status()
            body = response.json()
        except Exception:
            continue

        status = str(body.get("status") or "").lower()
        if status in SUCCESS_STATUSES:
            outputs = body.get("outputs") or {}
            output_gcs_path = outputs.get("json_folder") or body.get("output_gcs_path") or save_json_folder
            if not output_gcs_path:
                error = "missing output_gcs_path in completion response"
                logger.log(
                    "inference",
                    "failed",
                    subtrial_id=subtrial_id,
                    protocol=group.protocol,
                    trait_type=group.inference_trait_type,
                    job_id=job_id,
                    errors=error,
                    response_body=body,
                )
                return InferenceJobResult(
                    group.protocol,
                    group.raw_trait_value,
                    group.canonical_trait_name,
                    group.inference_trait_type,
                    False,
                    job_id,
                    None,
                    source_document_ids=group.source_document_ids,
                    source_collection_paths=group.source_collection_paths,
                    image_prefixes=group.image_prefixes,
                    error=error,
                )
            logger.log(
                "inference",
                "success",
                subtrial_id=subtrial_id,
                protocol=group.protocol,
                trait_type=group.inference_trait_type,
                job_id=job_id,
                output_gcs_path=output_gcs_path,
            )
            return InferenceJobResult(
                group.protocol,
                group.raw_trait_value,
                group.canonical_trait_name,
                group.inference_trait_type,
                True,
                job_id,
                output_gcs_path,
                annotated_output_gcs_path=outputs.get("output_folder") or save_output_folder,
                source_document_ids=group.source_document_ids,
                source_collection_paths=group.source_collection_paths,
                image_prefixes=group.image_prefixes,
            )
        if status in FAILED_STATUSES:
            logger.log(
                "inference",
                "failed",
                subtrial_id=subtrial_id,
                protocol=group.protocol,
                trait_type=group.inference_trait_type,
                job_id=job_id,
                response_body=body,
            )
            return InferenceJobResult(
                group.protocol,
                group.raw_trait_value,
                group.canonical_trait_name,
                group.inference_trait_type,
                False,
                job_id,
                None,
                source_document_ids=group.source_document_ids,
                source_collection_paths=group.source_collection_paths,
                image_prefixes=group.image_prefixes,
                error=str(body),
            )


def run_inference(
    config: AppConfig,
    *,
    run_id: str,
    subtrial_id: str,
    groups: list[ProtocolTraitGroup],
    logger,
    sleep: Callable[[float], None] = time.sleep,
) -> list[InferenceJobResult]:
    if not groups:
        return []

    results: list[InferenceJobResult] = []
    with ThreadPoolExecutor(max_workers=min(len(groups), 8)) as executor:
        futures = [
            executor.submit(
                _submit_and_poll_one,
                config,
                run_id=run_id,
                subtrial_id=subtrial_id,
                group=group,
                logger=logger,
                sleep=sleep,
            )
            for group in groups
        ]
        for future in as_completed(futures):
            results.append(future.result())
    return results
