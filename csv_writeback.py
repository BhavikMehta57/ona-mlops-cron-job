from __future__ import annotations

import csv
import io
from collections import OrderedDict
from typing import Any

from cron_job.gcs_client import download_text, list_blob_uris, parse_gcs_uri, upload_text
from cron_job.models import ExtractionRunResult


KEY_COLUMNS = {"document_id", "plot_uid", "plot", "plot_id", "plot_barcode"}
EXCLUDED_RESULT_COLUMNS = {
    "trial_name",
    "site_name",
    "season",
    "field",
    "location",
    "protocol",
    "date",
    "trait",
}


def _read_csv_text(text: str) -> tuple[list[str], list[dict[str, str]]]:
    reader = csv.DictReader(io.StringIO(text))
    return list(reader.fieldnames or []), [dict(row) for row in reader]


def _write_csv_text(columns: list[str], rows: list[dict[str, Any]]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row.get(column, "") for column in columns})
    return buffer.getvalue()


def _row_keys(row: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for key in ("document_id", "plot_uid", "plot", "plot_id", "plot_barcode"):
        value = str(row.get(key) or "").strip()
        if value:
            keys.append(value)
    return keys


def _result_column_name(column: str, trait: str, existing: set[str]) -> str:
    base = f"{column}__{trait}"
    if base not in existing:
        return base
    idx = 2
    while f"{base}_{idx}" in existing:
        idx += 1
    return f"{base}_{idx}"


def _plot_level_csvs(storage_client, output_prefix: str) -> list[str]:
    bucket, prefix = parse_gcs_uri(output_prefix.rstrip("/") + "/placeholder")
    prefix = prefix.rsplit("/", 1)[0].rstrip("/") + "/"
    uris = list_blob_uris(storage_client, bucket, prefix)
    return [
        uri
        for uri in uris
        if uri.lower().endswith(".csv")
        and ("plot-level" in uri.lower() or "plot_level" in uri.lower())
    ]


def _load_extraction_rows(
    storage_client,
    extraction_results: list[ExtractionRunResult],
    logger,
    subtrial_id: str,
) -> tuple[OrderedDict[str, str], dict[str, dict[str, str]]]:
    appended_columns: OrderedDict[str, str] = OrderedDict()
    values_by_key: dict[str, dict[str, str]] = {}
    existing_names: set[str] = set()

    for result in extraction_results:
        if not result.success or not result.output_prefix:
            continue
        try:
            csv_uris = _plot_level_csvs(storage_client, result.output_prefix)
        except Exception as exc:
            logger.log(
                "csv_writeback",
                "warning",
                subtrial_id=subtrial_id,
                output_prefix=result.output_prefix,
                exc=exc,
            )
            continue
        for uri in csv_uris:
            try:
                columns, rows = _read_csv_text(download_text(storage_client, uri))
            except Exception as exc:
                logger.log("csv_writeback", "warning", subtrial_id=subtrial_id, gcs_uri=uri, exc=exc)
                continue
            result_column_map: dict[str, str] = {}
            for column in columns:
                if column in KEY_COLUMNS or column in EXCLUDED_RESULT_COLUMNS:
                    continue
                mapped = _result_column_name(column, result.canonical_trait_name, existing_names)
                existing_names.add(mapped)
                appended_columns.setdefault(mapped, mapped)
                result_column_map[column] = mapped

            for row in rows:
                keys = _row_keys(row)
                if not keys:
                    continue
                for key in keys:
                    slot = values_by_key.setdefault(key, {})
                    for source_col, target_col in result_column_map.items():
                        if target_col not in slot:
                            slot[target_col] = row.get(source_col, "")
    return appended_columns, values_by_key


def write_back_csv(
    storage_client,
    *,
    raw_csv_uri: str,
    extraction_results: list[ExtractionRunResult],
    csv_type: str,
    logger,
    trial_id: str,
    subtrial_id: str,
) -> bool:
    try:
        raw_text = download_text(storage_client, raw_csv_uri)
        raw_columns, raw_rows = _read_csv_text(raw_text)
    except Exception as exc:
        logger.log(
            "csv_writeback",
            "failed",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            csv_type=csv_type,
            gcs_uri=raw_csv_uri,
            exc=exc,
        )
        return False

    appended_columns, values_by_key = _load_extraction_rows(
        storage_client,
        extraction_results,
        logger,
        subtrial_id,
    )
    final_columns = list(raw_columns)
    for column in appended_columns:
        if column not in final_columns:
            final_columns.append(column)

    for row in raw_rows:
        merged_values: dict[str, str] = {}
        for key in _row_keys(row):
            if key in values_by_key:
                merged_values = values_by_key[key]
                break
        for column in appended_columns:
            row[column] = merged_values.get(column, "")

    try:
        upload_text(storage_client, raw_csv_uri, _write_csv_text(final_columns, raw_rows), content_type="text/csv")
        logger.log(
            "csv_writeback",
            "success",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            csv_type=csv_type,
            gcs_uri=raw_csv_uri,
            appended_column_count=len(appended_columns),
        )
        return True
    except Exception as exc:
        logger.log(
            "csv_writeback",
            "failed",
            trial_id=trial_id,
            subtrial_id=subtrial_id,
            csv_type=csv_type,
            gcs_uri=raw_csv_uri,
            exc=exc,
        )
        return False
