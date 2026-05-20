"""Tests for the multi-date orchestration in main._process_multi_date.

These verify the new feature contract:
- Phase 1 runs scan/preprocess/inference per-date sequentially
- Phase 2 runs CV extraction once with combined inference results
- Phase 2 uploads ONE classical CSV per trait group (not one combined)
- Per-date failures don't abort remaining dates
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from cron_job import main as cron_main
from cron_job.schemas.models import (
    InferenceJobResult,
    PreprocessorResult,
    RunContext,
    ScannedDocument,
    SubtrialDocuments,
)

from .conftest import (
    CaptureLogger,
    make_classical_doc,
    make_image_doc,
    make_inference_result,
    make_subtrial_info,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_context(run_id: str = "run-1") -> RunContext:
    return RunContext(
        run_id=run_id,
        utc_date=date(2026, 5, 14),
        started_at=datetime(2026, 5, 14, tzinfo=timezone.utc),
    )


def _patch_no_op_phase1_helpers(monkeypatch, *, scan_returns):
    """Patch the helpers that phase 1 calls, returning per-date data."""

    calls = {"scan": []}

    def fake_scan(_db, _subtrial, _utc_date, _logger, *, data_collected_date=None):
        calls["scan"].append(data_collected_date)
        return scan_returns(data_collected_date)

    monkeypatch.setattr(cron_main, "scan_subtrial_documents", fake_scan)
    monkeypatch.setattr(cron_main, "upload_csv", lambda *_a, **_kw: "gs://bucket/raw/per-date.csv")
    monkeypatch.setattr(
        cron_main,
        "run_preprocessing",
        lambda *_a, **kw: PreprocessorResult(
            success=True,
            batch_run_id=kw.get("batch_run_id", "batch"),
            status="success",
            selected_shard_uris=["gs://bucket/selected.csv"],
        ),
    )
    monkeypatch.setattr(cron_main, "read_selected_image_rows", lambda *_a, **_kw: {})
    monkeypatch.setattr(
        cron_main,
        "build_selected_image_prefixes",
        lambda **kw: (
            # _selected_image_docs filters by _meta_image_uuid(doc.fields, doc.document_id),
            # which extracts the stem of doc.fields["gcs_img_path"]. Return that stem here.
            {
                __import__("pathlib", fromlist=["PurePosixPath"])
                .PurePosixPath(doc.fields.get("gcs_img_path") or "")
                .stem
                or doc.document_id
                for doc in kw["image_documents"]
            },
            {doc.document_id: "gs://bucket/x/images/sub/" for doc in kw["image_documents"]},
        ),
    )
    monkeypatch.setattr(cron_main, "write_back_preprocessing_status", lambda *_a, **_kw: 0)
    monkeypatch.setattr(cron_main, "write_back_inference_results", lambda *_a, **_kw: 0)
    return calls


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_multi_date_phase1_scans_each_date_sequentially(monkeypatch, app_config):
    """The phase-1 loop calls scan_subtrial_documents once per date, with the
    per-date `data_collected_date` keyword."""

    docs_by_date: dict[str, SubtrialDocuments] = {}

    def scan_returns(dc_date):
        # No documents for any date -> phase 2 just runs with nothing.
        result = SubtrialDocuments()
        docs_by_date[dc_date] = result
        return result

    calls = _patch_no_op_phase1_helpers(monkeypatch, scan_returns=scan_returns)
    # Phase-2 helpers won't be needed because nothing accumulated.
    monkeypatch.setattr(cron_main, "run_cv_extractions", lambda *_a, **_kw: [])
    monkeypatch.setattr(cron_main, "upload_classical_csv", lambda *_a, **_kw: None)
    monkeypatch.setattr(cron_main, "run_classical_extractions", lambda *_a, **_kw: [])
    monkeypatch.setattr(
        cron_main, "group_documents_by_protocol_trait", lambda *_a, **_kw: []
    )

    state = cron_main._process_multi_date(
        app_config,
        db=object(),
        storage_client=object(),
        cloud_run_client=object(),
        context=_run_context(),
        subtrial=make_subtrial_info(),
        dates=[date(2026, 5, 14), date(2026, 5, 15), date(2026, 5, 17)],
        logger=CaptureLogger(),
    )

    # scan was called once per supplied date with the correct ISO string.
    assert calls["scan"] == ["2026-05-14", "2026-05-15", "2026-05-17"]
    assert state.cv_path_status == "not_applicable"
    assert state.classical_path_status == "not_applicable"


def test_multi_date_phase2_runs_cv_extraction_with_combined_inference_results(
    monkeypatch, app_config
):
    """Phase 2 hands the *combined* list of inference results to run_cv_extractions
    so the dedup and batching happens in a single call."""

    image_doc = make_image_doc("img-1")

    def scan_returns(dc_date):
        return SubtrialDocuments(image_documents=[image_doc])

    _patch_no_op_phase1_helpers(monkeypatch, scan_returns=scan_returns)

    # Inference returns 1 successful result per date.
    monkeypatch.setattr(
        cron_main,
        "run_inference",
        lambda *_a, **_kw: [
            make_inference_result(
                output_path=f"gs://b/inference/{_kw.get('subtrial_id', 'sub')}/pods/RGB/json/"
            )
        ],
    )
    # Group resolution returns 1 valid group, but we ignore it for CV path.
    monkeypatch.setattr(
        cron_main,
        "group_documents_by_protocol_trait",
        lambda *_a, **_kw: [
            __import__("cron_job.schemas.models", fromlist=["ProtocolTraitGroup"]).ProtocolTraitGroup(
                protocol="RGB",
                raw_trait_value="pods",
                canonical_trait_name="pods",
                inference_trait_type="pod",
                source_document_ids=["img-1"],
                source_collection_paths=[],
                image_prefixes=["gs://bucket/x/images/sub/"],
            )
        ],
    )

    captured: dict = {}

    def fake_cv_extractions(_config, **kw):
        captured["count"] = len(kw["inference_results"])
        return [
            type(
                "ER",
                (),
                {"success": True, "output_prefix": "gs://b/extraction/", "canonical_trait_name": "pods"},
            )()
        ]

    monkeypatch.setattr(cron_main, "run_cv_extractions", fake_cv_extractions)
    # No classical docs, so these won't run.
    monkeypatch.setattr(cron_main, "upload_classical_csv", lambda *_a, **_kw: None)
    monkeypatch.setattr(cron_main, "run_classical_extractions", lambda *_a, **_kw: [])

    cron_main._process_multi_date(
        app_config,
        db=object(),
        storage_client=object(),
        cloud_run_client=object(),
        context=_run_context(),
        subtrial=make_subtrial_info(),
        dates=[date(2026, 5, 14), date(2026, 5, 15)],
        logger=CaptureLogger(),
    )

    # 2 dates × 1 inference result each = 2 results combined into ONE cv-extraction call.
    assert captured["count"] == 2


def test_multi_date_phase2_uploads_one_classical_csv_per_trait_group(
    monkeypatch, app_config
):
    """The new contract: each trait group gets its own filtered classical CSV
    uploaded, then a separate run_classical_extractions call per group."""

    flowering_doc = make_classical_doc(
        "flw-1", collection_name="flowering_data", trait="Flowering Date"
    )
    pods_doc = make_classical_doc(
        "pod-1", collection_name="numeric_data", trait="Pod Count"
    )

    def scan_returns(dc_date):
        if dc_date == "2026-05-14":
            return SubtrialDocuments(classical_documents=[flowering_doc])
        return SubtrialDocuments(classical_documents=[pods_doc])

    _patch_no_op_phase1_helpers(monkeypatch, scan_returns=scan_returns)

    monkeypatch.setattr(cron_main, "run_inference", lambda *_a, **_kw: [])
    monkeypatch.setattr(cron_main, "run_cv_extractions", lambda *_a, **_kw: [])

    # Group resolution: real implementation called via the scan path will produce
    # one group per trait. We use the actual function so it correctly groups by trait.
    upload_calls: list[list[str]] = []

    def fake_upload_classical(*_a, **kw):
        upload_calls.append([d.document_id for d in kw["documents"]])
        # Return a unique URI per call so downstream extraction sees it.
        return f"gs://bucket/classical-{len(upload_calls)}.csv"

    monkeypatch.setattr(cron_main, "upload_classical_csv", fake_upload_classical)

    extraction_calls: list[dict] = []

    def fake_classical_extract(_config, **kw):
        extraction_calls.append({"input_csv": kw["input_csv"], "groups": list(kw["groups"])})
        return [
            type(
                "ER",
                (),
                {
                    "success": True,
                    "output_prefix": kw["input_csv"],
                    "canonical_trait_name": kw["groups"][0].canonical_trait_name,
                },
            )()
        ]

    monkeypatch.setattr(cron_main, "run_classical_extractions", fake_classical_extract)

    state = cron_main._process_multi_date(
        app_config,
        db=object(),
        storage_client=object(),
        cloud_run_client=object(),
        context=_run_context(),
        subtrial=make_subtrial_info(),
        dates=[date(2026, 5, 14), date(2026, 5, 15)],
        logger=CaptureLogger(),
    )

    # Two trait groups (flowering, pods), so two classical CSV uploads
    # and two classical extraction calls.
    assert len(upload_calls) == 2
    assert len(extraction_calls) == 2

    # Each extraction call received exactly one group (no mixed protocol/data_type).
    for call in extraction_calls:
        assert len(call["groups"]) == 1

    # Classical path succeeded with both extractions.
    assert state.classical_path_status == "succeeded"


def test_multi_date_continues_after_per_date_failure(monkeypatch, app_config):
    """If scanning raises for one date, remaining dates still run."""

    def scan_returns(dc_date):
        if dc_date == "2026-05-15":
            raise RuntimeError("scan boom for the middle date")
        return SubtrialDocuments(image_documents=[make_image_doc("img-1")])

    _patch_no_op_phase1_helpers(monkeypatch, scan_returns=scan_returns)

    monkeypatch.setattr(
        cron_main,
        "run_inference",
        lambda *_a, **_kw: [make_inference_result()],
    )
    monkeypatch.setattr(
        cron_main,
        "group_documents_by_protocol_trait",
        lambda *_a, **_kw: [
            __import__("cron_job.schemas.models", fromlist=["ProtocolTraitGroup"]).ProtocolTraitGroup(
                protocol="RGB",
                raw_trait_value="pods",
                canonical_trait_name="pods",
                inference_trait_type="pod",
                source_document_ids=["img-1"],
                source_collection_paths=[],
                image_prefixes=["gs://bucket/x/images/sub/"],
            )
        ],
    )
    captured: dict = {}

    def fake_cv(_config, **kw):
        captured["count"] = len(kw["inference_results"])
        return [
            type(
                "ER",
                (),
                {"success": True, "output_prefix": "gs://b/", "canonical_trait_name": "pods"},
            )()
        ]

    monkeypatch.setattr(cron_main, "run_cv_extractions", fake_cv)

    logger = CaptureLogger()
    state = cron_main._process_multi_date(
        app_config,
        db=object(),
        storage_client=object(),
        cloud_run_client=object(),
        context=_run_context(),
        subtrial=make_subtrial_info(),
        dates=[date(2026, 5, 14), date(2026, 5, 15), date(2026, 5, 17)],
        logger=logger,
    )

    # Two dates produced inference results (the middle one threw).
    assert captured["count"] == 2
    # The middle date's failure is logged but doesn't abort the run.
    failed_phase1 = logger.find(stage="multi_date_phase1", status="failed")
    assert any(e.get("data_collected_date") == "2026-05-15" for e in failed_phase1)
    # Overall subtrial still succeeds because phase 2 produced extractable results.
    assert state.status == "succeeded"
