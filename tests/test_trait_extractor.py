"""Tests for services/trait_extractor.py.

Covers:
- Extraction RunSpec construction (method, trait, planting_date when applicable)
- Deduplication of CV inference outputs by (canonical_trait, output_path)
- Per-group classical extraction calls
- Polling terminal state mapping
"""
from __future__ import annotations

from cron_job.services import trait_extractor
from cron_job.services.trait_extractor import (
    run_classical_extractions,
    run_cv_extractions,
)

from .conftest import CaptureLogger, make_group, make_inference_result


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class CapturingCloudRunClient:
    """Stand-in for CloudRunJobClient that captures inline run_specs."""

    class _Ref:
        job_name = "projects/p/locations/r/jobs/j"

    def __init__(self):
        self.ref = self._Ref()
        self.run_specs: list[dict] = []

    def run_job_with_inline_spec(self, run_spec):
        self.run_specs.append(run_spec)
        return {
            "name": f"operations/op-{len(self.run_specs)}",
            "metadata": {"name": f"projects/p/locations/r/jobs/j/executions/exec-{len(self.run_specs)}"},
        }


class FakeBatch:
    def __init__(self):
        self.writes: list[tuple[str, dict]] = []

    def set(self, ref, payload, merge=True):
        self.writes.append((str(ref), payload))

    def commit(self):
        return None


class FakeFirestoreDb:
    """Minimal Firestore-like db used by audit-record writes inside trait_extractor."""

    def __init__(self):
        self.collections: dict[str, dict] = {}

    def collection(self, name):
        client = self
        store = client.collections.setdefault(name, {})

        class Collection:
            def document(self, doc_id):
                store.setdefault(doc_id, {})

                class Doc:
                    def set(self_inner, payload, merge=False):
                        store[doc_id].update(payload)

                return Doc()

        return Collection()

    def batch(self):
        return FakeBatch()

    def document(self, _path):
        class _Ref:
            pass

        return _Ref()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_polling_to_complete(monkeypatch):
    """Replace _poll_execution so we don't actually loop or sleep."""
    monkeypatch.setattr(
        trait_extractor,
        "_poll_execution",
        lambda *_a, **_kw: ("completed", "executions/exec-1", None),
    )


# ---------------------------------------------------------------------------
# CV extractions
# ---------------------------------------------------------------------------


def test_cv_extractions_dedupes_by_trait_and_output_path(monkeypatch, app_config):
    _patch_polling_to_complete(monkeypatch)
    cr_client = CapturingCloudRunClient()
    db = FakeFirestoreDb()
    duplicate_path = "gs://ona-harvest/inference/run-1/sub/pods/RGB/json/"
    inference_results = [
        make_inference_result(canonical_trait="pods", output_path=duplicate_path),
        make_inference_result(canonical_trait="pods", output_path=duplicate_path),
        make_inference_result(
            canonical_trait="flowering",
            inference_trait_type="flower",
            output_path="gs://ona-harvest/inference/run-1/sub/flowering/RGB/json/",
        ),
    ]

    results = run_cv_extractions(
        app_config,
        db=db,
        cloud_run_client=cr_client,
        run_id="run-1",
        subtrial_id="sub",
        inference_results=inference_results,
        logger=CaptureLogger(),
        sleep=lambda _: None,
    )

    # Only 2 unique (trait, output_path) pairs => 2 cloud-run invocations.
    assert len(cr_client.run_specs) == 2
    assert len(results) == 2
    methods = {spec["method"] for spec in cr_client.run_specs}
    assert methods == {"computer_vision"}


def test_cv_extractions_skips_failed_or_pathless_inference_results(monkeypatch, app_config):
    _patch_polling_to_complete(monkeypatch)
    cr_client = CapturingCloudRunClient()
    inference_results = [
        make_inference_result(success=False),
        make_inference_result(output_path=""),  # falsy path
        make_inference_result(),  # the only valid one
    ]

    run_cv_extractions(
        app_config,
        db=FakeFirestoreDb(),
        cloud_run_client=cr_client,
        run_id="run-1",
        subtrial_id="sub",
        inference_results=inference_results,
        logger=CaptureLogger(),
        sleep=lambda _: None,
    )

    assert len(cr_client.run_specs) == 1


def test_cv_extractions_attaches_planting_date_for_flowering_only(monkeypatch, app_config):
    _patch_polling_to_complete(monkeypatch)
    cr_client = CapturingCloudRunClient()
    inference_results = [
        make_inference_result(
            canonical_trait="flowering",
            inference_trait_type="flower",
            output_path="gs://ona-harvest/inference/run-1/sub/flowering/RGB/json/",
        ),
        make_inference_result(canonical_trait="pods"),
    ]

    run_cv_extractions(
        app_config,
        db=FakeFirestoreDb(),
        cloud_run_client=cr_client,
        run_id="run-1",
        subtrial_id="sub",
        inference_results=inference_results,
        logger=CaptureLogger(),
        planting_date="2026-04-01",
        sleep=lambda _: None,
    )

    flowering_spec = next(s for s in cr_client.run_specs if s["trait"] == "flowering")
    pods_spec = next(s for s in cr_client.run_specs if s["trait"] == "pods")
    assert flowering_spec["planting_date"] == "2026-04-01"
    assert "planting_date" not in pods_spec


# ---------------------------------------------------------------------------
# Classical extractions
# ---------------------------------------------------------------------------


def test_classical_extractions_run_once_per_group_with_input_csv(monkeypatch, app_config):
    _patch_polling_to_complete(monkeypatch)
    cr_client = CapturingCloudRunClient()
    pods_group = make_group(canonical_trait="pods", inference_trait_type="pod")
    flowering_group = make_group(canonical_trait="flowering", inference_trait_type="flower")

    run_classical_extractions(
        app_config,
        db=FakeFirestoreDb(),
        cloud_run_client=cr_client,
        run_id="run-1",
        subtrial_id="sub",
        input_csv="gs://ona-harvest/raw/run-1_classical.csv",
        groups=[pods_group, flowering_group],
        planting_date="2026-04-01",
        logger=CaptureLogger(),
        sleep=lambda _: None,
    )

    assert len(cr_client.run_specs) == 2
    by_trait = {spec["trait"]: spec for spec in cr_client.run_specs}
    assert by_trait["pods"]["method"] == "classical"
    assert by_trait["pods"]["input_csv"] == "gs://ona-harvest/raw/run-1_classical.csv"
    assert "planting_date" not in by_trait["pods"]
    # flowering group gets planting_date attached.
    assert by_trait["flowering"]["planting_date"] == "2026-04-01"


def test_classical_extractions_returns_failed_result_when_post_fails(monkeypatch, app_config):
    """If run_job_with_inline_spec raises, the per-group result captures the error."""

    class FailingClient:
        class _Ref:
            job_name = "projects/p/locations/r/jobs/j"

        def __init__(self):
            self.ref = self._Ref()

        def run_job_with_inline_spec(self, _spec):
            raise RuntimeError("api boom")

    group = make_group(canonical_trait="pods", inference_trait_type="pod")

    results = run_classical_extractions(
        app_config,
        db=FakeFirestoreDb(),
        cloud_run_client=FailingClient(),
        run_id="run-1",
        subtrial_id="sub",
        input_csv="gs://ona-harvest/raw/run-1_classical.csv",
        groups=[group],
        planting_date=None,
        logger=CaptureLogger(),
        sleep=lambda _: None,
    )

    assert len(results) == 1
    assert results[0].success is False
    assert "api boom" in (results[0].error or "")


# ---------------------------------------------------------------------------
# _execution_status edge cases (in addition to those in test_daily_pipeline_adapters)
# ---------------------------------------------------------------------------


def test_execution_status_cancelled_when_cancellations_present():
    status, _, _, _, _ = trait_extractor._execution_status(
        {
            "taskCount": 1,
            "succeededCount": 0,
            "failedCount": 0,
            "cancelledCount": 1,
            "completionTime": "done",
        }
    )
    assert status == "cancelled"


def test_execution_status_cancel_requested_when_cancellation_in_progress():
    status, _, _, _, _ = trait_extractor._execution_status(
        {
            "taskCount": 1,
            "succeededCount": 0,
            "failedCount": 0,
            "cancelledCount": 1,
        }
    )
    assert status == "cancel_requested"
