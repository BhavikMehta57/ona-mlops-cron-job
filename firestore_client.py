from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from cron_job.csv_assembler import _image_uri_from_fields
from cron_job.models import ScannedDocument, SubtrialDocuments, SubtrialInfo


def _doc_to_dict(snapshot) -> dict[str, Any]:
    data = snapshot.to_dict() or {}
    data["id"] = snapshot.id
    return data


def _make_trial_id(layout: dict[str, Any]) -> str | None:
    trial_name = str(layout.get("trial_name") or "").strip()
    site_name = str(layout.get("site_name") or "").strip()
    if not trial_name or not site_name:
        return None
    return f"{trial_name}--{site_name}"


def _make_subtrial_id(layout: dict[str, Any]) -> str | None:
    season = str(layout.get("season") or "").strip()
    field = str(layout.get("field") or "").strip()
    location = str(layout.get("location") or "").strip()
    if not season or not field or not location:
        return None
    return f"{season}--{field}--{location}"


def discover_active_subtrials(db, logger) -> list[SubtrialInfo]:
    """Discover active subtrials from pipeline-enabled trial layouts."""
    discovered: list[SubtrialInfo] = []
    seen: set[tuple[str, str]] = set()

    try:
        layout_snaps = list(db.collection("trial_layouts").where("pipelineEnabled", "==", True).stream())
    except Exception as exc:
        logger.log("discovery", "fatal", exc=exc)
        raise

    if not layout_snaps:
        logger.log("discovery", "no_active_trials", total_subtrials_discovered=0)
        return []

    for snap in layout_snaps:
        layout = _doc_to_dict(snap)
        trial_id = _make_trial_id(layout)
        subtrial_id = _make_subtrial_id(layout)
        if not trial_id or not subtrial_id:
            logger.log(
                "discovery",
                "skipped",
                trial_layout_id=snap.id,
                errors="trial_layout missing trial/subtrial identity fields",
            )
            continue

        key = (trial_id, subtrial_id)
        if key in seen:
            continue
        seen.add(key)

        trial_ref = db.collection("trials").document(trial_id)
        trial_snap = trial_ref.get()
        if not trial_snap.exists:
            logger.log(
                "discovery",
                "skipped",
                trial_id=trial_id,
                subtrial_id=subtrial_id,
                trial_layout_id=snap.id,
                errors="matching trial hierarchy document not found",
            )
            continue

        subtrial_ref = trial_ref.collection("subtrials").document(subtrial_id)
        subtrial_snap = subtrial_ref.get()
        if not subtrial_snap.exists:
            logger.log(
                "discovery",
                "skipped",
                trial_id=trial_id,
                subtrial_id=subtrial_id,
                trial_layout_id=snap.id,
                errors="matching subtrial hierarchy document not found",
            )
            continue

        discovered.append(
            SubtrialInfo(
                trial_id=trial_id,
                subtrial_id=subtrial_id,
                trial_layout_id=snap.id,
                trial_data=trial_snap.to_dict() or {},
                subtrial_data=subtrial_snap.to_dict() or {},
                layout_data=layout,
            )
        )

    logger.log(
        "discovery",
        "complete",
        total_subtrials_discovered=len(discovered),
        active_trial_layout_count=len(layout_snaps),
    )
    return discovered


def utc_day_window(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _is_in_window(value: Any, start: datetime, end: datetime) -> bool:
    if value is None:
        return False
    if isinstance(value, datetime):
        ts = value
    elif hasattr(value, "timestamp"):
        ts = datetime.fromtimestamp(value.timestamp(), tz=timezone.utc)
    else:
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return start <= ts < end


def _query_by_timestamp(collection_ref, start: datetime, end: datetime):
    return (
        collection_ref.where("upload_timestamp", ">=", start)
        .where("upload_timestamp", "<", end)
        .stream()
    )


def _query_by_data_collection_date(collection_ref, target_date: str):
    """Query documents where data_collection string starts with target_date (YYYY-MM-DD).

    Uses a Firestore range query on the string field:
    data_collection >= "2025-11-03" AND data_collection < "2025-11-04"
    This matches all values starting with that date prefix.
    """
    next_day = str(date.fromisoformat(target_date) + timedelta(days=1))
    return (
        collection_ref.where("data_collection", ">=", target_date)
        .where("data_collection", "<", next_day)
        .stream()
    )


def _matches_data_collection_date(fields: dict[str, Any], target_date: str) -> bool:
    """Check if a document's data_collection field starts with the target date."""
    dc = str(fields.get("data_collection") or "").strip()
    return dc.startswith(target_date)


def _scan_classical_collection(
    st_ref,
    collection_name: str,
    start: datetime,
    end: datetime,
    logger,
    trial_id: str,
    subtrial_id: str,
    *,
    data_collected_date: str | None = None,
) -> tuple[list[ScannedDocument], list[str]]:
    out: list[ScannedDocument] = []
    errors: list[str] = []
    try:
        if data_collected_date:
            snaps = list(_query_by_data_collection_date(st_ref.collection(collection_name), data_collected_date))
        else:
            snaps = list(_query_by_timestamp(st_ref.collection(collection_name), start, end))
    except Exception as exc:
        message = f"{collection_name}: {exc}"
        errors.append(message)
        logger.log(
            "scanning",
            "error",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            collection_name=collection_name,
            exc=exc,
        )
        return out, errors

    for snap in snaps:
        fields = snap.to_dict() or {}
        if data_collected_date:
            if not _matches_data_collection_date(fields, data_collected_date):
                continue
            upload_ts = fields.get("upload_timestamp")
        else:
            upload_ts = fields.get("upload_timestamp")
            if not _is_in_window(upload_ts, start, end):
                continue
        out.append(
            ScannedDocument(
                collection_name=collection_name,
                document_id=snap.id,
                collection_path=snap.reference.path,
                protocol_date_id=None,
                image_uri=None,
                protocol=fields.get("protocol"),
                trait=fields.get("trait"),
                upload_timestamp=upload_ts,
                fields=fields,
            )
        )
    return out, errors


def _scan_images(
    st_ref,
    start: datetime,
    end: datetime,
    logger,
    trial_id: str,
    subtrial_id: str,
    *,
    data_collected_date: str | None = None,
) -> tuple[list[ScannedDocument], list[str]]:
    out: list[ScannedDocument] = []
    errors: list[str] = []

    try:
        protocol_refs = list(st_ref.collection("images").list_documents(page_size=250))
    except Exception as exc:
        logger.log(
            "scanning",
            "error",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            collection_name="images",
            exc=exc,
        )
        return out, [f"images: {exc}"]

    for protocol_ref in protocol_refs:
        try:
            protocol_snap = protocol_ref.get()
            protocol_data = protocol_snap.to_dict() or {}
            protocol = protocol_data.get("protocol") or protocol_ref.id.split("--", 1)[0]
            trait = protocol_data.get("trait")

            for plot_col in protocol_ref.collections():
                try:
                    if data_collected_date:
                        snaps = list(_query_by_data_collection_date(plot_col, data_collected_date))
                    else:
                        snaps = list(_query_by_timestamp(plot_col, start, end))
                except Exception as exc:
                    errors.append(f"{plot_col.id}: {exc}")
                    logger.log(
                        "scanning",
                        "error",
                        trial_id=trial_id,
                        subtrial_id=subtrial_id,
                        collection_name="images",
                        protocol_date_id=protocol_ref.id,
                        plot_uid=plot_col.id,
                        exc=exc,
                    )
                    continue

                for snap in snaps:
                    fields = snap.to_dict() or {}
                    if data_collected_date:
                        if not _matches_data_collection_date(fields, data_collected_date):
                            continue
                        upload_ts = fields.get("upload_timestamp")
                    else:
                        upload_ts = fields.get("upload_timestamp")
                        if not _is_in_window(upload_ts, start, end):
                            continue
                    fields.setdefault("protocol", protocol)
                    fields.setdefault("trait", trait)
                    out.append(
                        ScannedDocument(
                            collection_name="images",
                            document_id=snap.id,
                            collection_path=snap.reference.path,
                            protocol_date_id=protocol_ref.id,
                            image_uri=_image_uri_from_fields(fields),
                            protocol=protocol,
                            trait=trait,
                            upload_timestamp=upload_ts,
                            fields=fields,
                        )
                    )
        except Exception as exc:
            errors.append(f"{protocol_ref.id}: {exc}")
            logger.log(
                "scanning",
                "error",
                trial_id=trial_id,
                subtrial_id=subtrial_id,
                collection_name="images",
                protocol_date_id=protocol_ref.id,
                exc=exc,
            )
    return out, errors


def _count_two_images(st_ref) -> int:
    try:
        return sum(1 for _ in st_ref.collection("two_images_with_count").list_documents(page_size=250))
    except Exception:
        return 0


def scan_subtrial_documents(db, subtrial: SubtrialInfo, utc_date: date, logger, *, data_collected_date: str | None = None) -> SubtrialDocuments:
    start, end = utc_day_window(utc_date)
    st_ref = (
        db.collection("trials")
        .document(subtrial.trial_id)
        .collection("subtrials")
        .document(subtrial.subtrial_id)
    )
    docs = SubtrialDocuments()

    images, image_errors = _scan_images(st_ref, start, end, logger, subtrial.trial_id, subtrial.subtrial_id, data_collected_date=data_collected_date)
    docs.image_documents.extend(images)
    docs.scan_errors.extend(image_errors)

    skipped_two_images_count = _count_two_images(st_ref)
    docs.skipped_two_images_count = skipped_two_images_count
    logger.log(
        "scanning",
        "skipped",
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        collection_name="two_images_with_count",
        skipped_count=skipped_two_images_count,
    )

    for collection_name in ("flowering_data", "numeric_data"):
        items, errors = _scan_classical_collection(
            st_ref,
            collection_name,
            start,
            end,
            logger,
            subtrial.trial_id,
            subtrial.subtrial_id,
            data_collected_date=data_collected_date,
        )
        docs.classical_documents.extend(items)
        docs.scan_errors.extend(errors)

    logger.log(
        "scanning",
        "complete",
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        document_count=len(docs.image_documents) + len(docs.classical_documents),
        image_document_count=len(docs.image_documents),
        classical_document_count=len(docs.classical_documents),
        skipped_two_images_count=docs.skipped_two_images_count,
        scan_error_count=len(docs.scan_errors),
    )
    return docs
