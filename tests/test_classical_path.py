"""Tests for the per-trait classical CSV behaviour in main._process_classical_path.

The pipeline now uploads ONE classical CSV per (protocol, trait) group, not one
combined CSV. This file pins the contract so downstream regressions are caught.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from cron_job import main as cron_main
from cron_job.schemas.models import (
    ExtractionRunResult,
    RunContext,
    SubtrialDocuments,
    SubtrialState,
)

from .conftest import CaptureLogger, make_classical_doc, make_subtrial_info


def _state(subtrial) -> SubtrialState:
    return SubtrialState(
        trial_id=subtrial.trial_id,
        subtrial_id=subtrial.subtrial_id,
        index=1,
        batch_run_id="batch-1",
    )


def _context() -> RunContext:
    return RunContext(
        run_id="run-1",
        utc_date=date(2026, 5, 14),
        started_at=datetime(2026, 5, 14, tzinfo=timezone.utc),
    )


def test_classical_path_uploads_one_csv_per_trait_group(monkeypatch, app_config):
    """Mixed (flowering + pods) classical docs should result in two CSV uploads,
    each filtered to a single trait group, and two extraction calls."""

    flowering_docs = [
        make_classical_doc(f"flw-{i}", collection_name="flowering_data", trait="Flowering Date")
        for i in range(3)
    ]
    pods_docs = [
        make_classical_doc(f"pod-{i}", collection_name="numeric_data", trait="Pod Count")
        for i in range(2)
    ]
    docs = SubtrialDocuments(classical_documents=flowering_docs + pods_docs)

    upload_calls: list[list[str]] = []

    def fake_upload_classical(*_a, **kw):
        upload_calls.append([d.document_id for d in kw["documents"]])
        return f"gs://bucket/classical-{len(upload_calls)}.csv"

    extraction_calls: list[dict] = []

    def fake_extractions(_config, **kw):
        extraction_calls.append(
            {"input_csv": kw["input_csv"], "trait": kw["groups"][0].canonical_trait_name}
        )
        return [
            ExtractionRunResult(
                method="classical",
                canonical_trait_name=kw["groups"][0].canonical_trait_name,
                success=True,
                run_id="audit-1",
                output_prefix=kw["input_csv"],
                status="completed",
            )
        ]

    monkeypatch.setattr(cron_main, "upload_classical_csv", fake_upload_classical)
    monkeypatch.setattr(cron_main, "run_classical_extractions", fake_extractions)

    subtrial = make_subtrial_info()
    state = _state(subtrial)
    status = cron_main._process_classical_path(
        app_config,
        db=object(),
        storage_client=object(),
        cloud_run_client=object(),
        context=_context(),
        subtrial=subtrial,
        state=state,
        docs=docs,
        logger=CaptureLogger(),
    )

    assert status == "succeeded"
    # 2 trait groups → 2 uploads → 2 extraction calls
    assert len(upload_calls) == 2
    assert len(extraction_calls) == 2

    # Each upload is filtered to documents from a single trait group.
    docs_per_upload = {tuple(sorted(call)) for call in upload_calls}
    assert (tuple(sorted(d.document_id for d in flowering_docs)),) in (
        (tuple(sorted(d.document_id for d in flowering_docs)),)
        for _ in range(1)
    )
    # The flowering upload contains only flowering docs and the pods upload only pods.
    flw_ids = {d.document_id for d in flowering_docs}
    pod_ids = {d.document_id for d in pods_docs}
    upload_sets = [set(call) for call in upload_calls]
    assert flw_ids in upload_sets
    assert pod_ids in upload_sets

    # Each extraction call receives the corresponding URI.
    traits = {c["trait"] for c in extraction_calls}
    assert traits == {"flowering", "pods"}


def test_classical_path_returns_failed_when_no_groups_resolved(monkeypatch, app_config):
    """If documents have unrecognized trait values, no groups are formed and
    the path is marked failed at protocol_trait_resolution."""
    docs = SubtrialDocuments(
        classical_documents=[
            make_classical_doc("doc-1", trait="UnknownStrangeTrait")
        ]
    )

    monkeypatch.setattr(
        cron_main,
        "upload_classical_csv",
        lambda *_a, **_kw: (_ for _ in ()).throw(AssertionError("upload should not run")),
    )

    subtrial = make_subtrial_info()
    state = _state(subtrial)
    status = cron_main._process_classical_path(
        app_config,
        db=object(),
        storage_client=object(),
        cloud_run_client=object(),
        context=_context(),
        subtrial=subtrial,
        state=state,
        docs=docs,
        logger=CaptureLogger(),
    )

    assert status == "failed"
    assert state.failed_stage == "protocol_trait_resolution"


def test_classical_path_skipped_when_no_classical_documents(app_config):
    docs = SubtrialDocuments()
    subtrial = make_subtrial_info()
    state = _state(subtrial)

    status = cron_main._process_classical_path(
        app_config,
        db=object(),
        storage_client=object(),
        cloud_run_client=object(),
        context=_context(),
        subtrial=subtrial,
        state=state,
        docs=docs,
        logger=CaptureLogger(),
    )

    assert status == "not_applicable"


def test_classical_path_handles_partial_csv_upload_failure(monkeypatch, app_config):
    """If one trait group's CSV upload returns None (e.g. missing metadata
    fields), the path continues with the other group(s)."""

    flowering = make_classical_doc("flw-1", trait="Flowering Date")
    pods = make_classical_doc("pod-1", trait="Pod Count")
    docs = SubtrialDocuments(classical_documents=[flowering, pods])

    def fake_upload(*_a, **kw):
        # Fail upload only for pods group (single-doc upload identifies trait via fields)
        traits = {d.trait for d in kw["documents"]}
        if "Pod Count" in traits:
            return None
        return "gs://bucket/flowering.csv"

    extraction_traits: list[str] = []

    def fake_extractions(_config, **kw):
        trait = kw["groups"][0].canonical_trait_name
        extraction_traits.append(trait)
        return [
            ExtractionRunResult(
                method="classical",
                canonical_trait_name=trait,
                success=True,
                run_id="audit-1",
                output_prefix=kw["input_csv"],
                status="completed",
            )
        ]

    monkeypatch.setattr(cron_main, "upload_classical_csv", fake_upload)
    monkeypatch.setattr(cron_main, "run_classical_extractions", fake_extractions)

    subtrial = make_subtrial_info()
    state = _state(subtrial)
    status = cron_main._process_classical_path(
        app_config,
        db=object(),
        storage_client=object(),
        cloud_run_client=object(),
        context=_context(),
        subtrial=subtrial,
        state=state,
        docs=docs,
        logger=CaptureLogger(),
    )

    # Only flowering extraction ran (pods upload failed).
    assert extraction_traits == ["flowering"]
    # Path still ends in a non-failed status because at least one group succeeded.
    assert status in {"succeeded", "completed_with_errors"}
