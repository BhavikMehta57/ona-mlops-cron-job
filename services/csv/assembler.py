from __future__ import annotations

import csv
import io
import json
from datetime import date, datetime
from pathlib import PurePosixPath
from typing import Any

from cron_job.services import gcs as gcs_client
from cron_job.schemas.models import ScannedDocument


LEADING_COLUMNS = ["collection_name", "document_id", "image_uri", "protocol", "trait"]
IMAGE_COMPAT_COLUMNS = ["img_bucket_prefix", "img_gcs_img_path", "meta_image_uuid"]
RESERVED_COLUMNS = set(LEADING_COLUMNS + IMAGE_COMPAT_COLUMNS)


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, default=str, sort_keys=True)
    return str(value)


def _image_uri_from_fields(fields: dict[str, Any]) -> str:
    gcs_path = str(fields.get("gcs_img_path") or "").strip()
    bucket_prefix = str(fields.get("bucket_prefix") or "").strip()
    if not gcs_path:
        return ""
    if gcs_path.startswith("gs://"):
        return gcs_path
    if bucket_prefix.startswith("gs://"):
        return f"{bucket_prefix.rstrip('/')}/{gcs_path.lstrip('/')}"
    if bucket_prefix:
        return f"gs://{bucket_prefix.strip('/')}/{gcs_path.lstrip('/')}"
    return gcs_path


def _meta_image_uuid(fields: dict[str, Any], document_id: str) -> str:
    gcs_path = str(fields.get("gcs_img_path") or "").strip()
    if gcs_path:
        return PurePosixPath(gcs_path).stem
    return str(fields.get("image_uuid") or document_id)


def document_to_row(doc: ScannedDocument) -> dict[str, str]:
    fields = dict(doc.fields)
    image_uri = doc.image_uri or _image_uri_from_fields(fields)

    row: dict[str, str] = {
        "collection_name": doc.collection_name,
        "document_id": doc.document_id,
        "image_uri": image_uri or "",
        "protocol": doc.protocol or "",
        "trait": doc.trait or "",
    }

    if doc.collection_name == "images":
        row.update(
            {
                "img_bucket_prefix": _stringify(fields.get("bucket_prefix")),
                "img_gcs_img_path": _stringify(fields.get("gcs_img_path")),
                "meta_image_uuid": _meta_image_uuid(fields, doc.document_id),
            }
        )

    for key, value in fields.items():
        if key in RESERVED_COLUMNS:
            continue
        row[key] = _stringify(value)

    return row


def assemble_csv(documents: list[ScannedDocument], *, csv_type: str) -> str:
    if not documents:
        return ""

    rows = [document_to_row(doc) for doc in documents]
    dynamic = sorted({key for row in rows for key in row if key not in RESERVED_COLUMNS})
    columns = list(LEADING_COLUMNS)
    if csv_type.startswith("images"):
        columns.extend(IMAGE_COMPAT_COLUMNS)
    columns.extend(dynamic)

    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row.get(column, "") for column in columns})
    return buffer.getvalue()


def upload_csv(
    storage_client,
    *,
    bucket_name: str,
    run_id: str,
    trial_id: str,
    subtrial_id: str,
    csv_type: str,
    documents: list[ScannedDocument],
    logger,
    raw_prefix_root: str = "raw",
) -> str | None:
    if not documents:
        logger.log(
            "csv_upload",
            "skipped",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            csv_type=csv_type,
            document_count=0,
        )
        return None

    csv_text = assemble_csv(documents, csv_type=csv_type)
    blob_name = f"{raw_prefix_root.strip('/') or 'raw'}/{run_id}_{csv_type}.csv"
    uri = gcs_client.make_gcs_uri(bucket_name, blob_name)

    try:
        gcs_client.upload_text(storage_client, uri, csv_text, content_type="text/csv")
        logger.log(
            "csv_upload",
            "success",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            csv_type=csv_type,
            document_count=len(documents),
            gcs_uri=uri,
        )
        return uri
    except Exception as exc:
        try:
            gcs_client.delete_blob(storage_client, uri)
        except Exception:
            pass
        logger.log(
            "csv_upload",
            "failed",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            csv_type=csv_type,
            document_count=len(documents),
            exc=exc,
        )
        return None


def _classical_phenotyping_path(documents: list[ScannedDocument], run_id: str, run_date: str) -> str | None:
    """Build the classical-phenotyping GCS path from document fields.

    Target: {project_name}/{site_name}/{trial_name}/{season}/{field}/{location}/trait_collection/classical-phenotyping/{run_id}_{run_date}_{epoch}.csv
    """
    import time as _time

    for doc in documents:
        fields = doc.fields
        project_name = str(fields.get("project_name") or "").strip()
        site_name = str(fields.get("site_name") or "").strip()
        trial_name = str(fields.get("trial_name") or "").strip()
        season = str(fields.get("season") or "").strip()
        field = str(fields.get("field") or "").strip()
        location = str(fields.get("location") or "").strip()
        if all([project_name, site_name, trial_name, season, field, location]):
            epoch = int(_time.time())
            csv_name = f"{run_id}_{run_date}_{epoch}.csv"
            return f"{project_name}/{site_name}/{trial_name}/{season}/{field}/{location}/trait_collection/classical-phenotyping/{csv_name}"
    return None


def upload_classical_csv(
    storage_client,
    *,
    bucket_name: str,
    run_id: str,
    run_date: str,
    trial_id: str,
    subtrial_id: str,
    documents: list[ScannedDocument],
    logger,
) -> str | None:
    """Upload classical CSV to the standard phenotyping path derived from document fields."""
    if not documents:
        logger.log(
            "csv_upload",
            "skipped",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            csv_type="classical",
            document_count=0,
        )
        return None

    blob_name = _classical_phenotyping_path(documents, run_id, run_date)
    if not blob_name:
        logger.log(
            "csv_upload",
            "failed",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            csv_type="classical",
            document_count=len(documents),
            errors="Could not derive classical-phenotyping path from document fields",
        )
        return None

    csv_text = assemble_csv(documents, csv_type="classical")
    uri = gcs_client.make_gcs_uri(bucket_name, blob_name)

    try:
        gcs_client.upload_text(storage_client, uri, csv_text, content_type="text/csv")
        logger.log(
            "csv_upload",
            "success",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            csv_type="classical",
            document_count=len(documents),
            gcs_uri=uri,
        )
        return uri
    except Exception as exc:
        try:
            gcs_client.delete_blob(storage_client, uri)
        except Exception:
            pass
        logger.log(
            "csv_upload",
            "failed",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            csv_type="classical",
            document_count=len(documents),
            exc=exc,
        )
        return None
