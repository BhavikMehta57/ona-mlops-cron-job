"""Adapter and CLI-level tests for the cron_job orchestrator.

Covers:
- argparse defaults and step-prefix overrides
- discovery short-circuit when no active layouts
- preprocessor request body and Firestore-backed shard discovery
- inference batch submission, polling, and output mapping
- CloudRun execution status mapping
- run() exit codes for failure / no-data / multi-date paths
"""
from __future__ import annotations

from cron_job import main as cron_main
from cron_job.schemas.models import (
    ProtocolTraitGroup,
    SubtrialInfo,
    SubtrialState,
)
from cron_job.services.inference import run_inference
from cron_job.services.preprocessor import run_preprocessing
from cron_job.services.trait_extractor import _execution_status

from .conftest import CaptureLogger


# ---------------------------------------------------------------------------
# Fakes for httpx + Firestore
# ---------------------------------------------------------------------------


class Response:
    def __init__(self, body, status_code=200, text=None):
        self._body = body
        self.status_code = status_code
        self.text = text if text is not None else str(body)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.text)

    def json(self):
        return self._body


class HttpClient:
    """A minimal httpx.Client stand-in usable as a context manager."""

    def __init__(self, *, post_body=None, get_body=None):
        self.post_body = post_body or {}
        self.get_body = get_body or {}
        self.posts: list[tuple[str, dict]] = []
        self.gets: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def post(self, url, json):
        self.posts.append((url, json))
        return Response(self.post_body)

    def get(self, url):
        self.gets.append(url)
        return Response(self.get_body)


class FakeSnapshot:
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return dict(self._data)


class FakeShardsCollection:
    def stream(self):
        return [
            FakeSnapshot(
                {
                    "selected_uri": "gs://bucket/selected.csv",
                    "selection_status": "selected",
                }
            )
        ]


class FakeRunDoc:
    def collection(self, name):
        assert name == "shards"
        return FakeShardsCollection()

    def get(self):
        return FakeSnapshot(
            {
                "shards": {
                    "legacy": {
                        "selected_uri": "gs://bucket/legacy.csv",
                        "selection_status": "selected",
                    }
                }
            }
        )


class FakePreprocessingDb:
    def collection(self, name):
        assert name == "preprocessing_runs"
        return self

    def document(self, doc_id):
        assert doc_id == "batch-1"
        return FakeRunDoc()


# ---------------------------------------------------------------------------
# argparse + step prefixes
# ---------------------------------------------------------------------------


def test_step_args_default_to_local_test_prefixes(app_config):
    args = cron_main.parse_args(["--step", "csv", "--run-id", "run-1"])

    config = cron_main._apply_local_step_prefixes(app_config, args)

    assert config.raw_prefix_root == "raw/local-test"
    assert config.inference_prefix_root == "inference/local-test"
    assert config.extraction_prefix_root == "extraction/local-test"


def test_data_collected_dates_parses_plus_separator():
    args = cron_main.parse_args(
        [
            "--data-collected-dates",
            "2026-05-14+2026-05-15+2026-05-17",
            "--trial-id",
            "T--S",
            "--subtrial-id",
            "2026--F--L",
        ]
    )

    assert [d.isoformat() for d in args.data_collected_dates] == [
        "2026-05-14",
        "2026-05-15",
        "2026-05-17",
    ]


def test_data_collected_dates_accepts_semicolon_separator():
    args = cron_main.parse_args(
        ["--data-collected-dates", "2026-05-14;2026-05-15"]
    )
    assert len(args.data_collected_dates) == 2


def test_step_discover_limits_results_without_cloud_run_client(monkeypatch, app_config):
    subtrials = [
        SubtrialInfo("T--S", "2026--F--L", "layout-1", {}, {}, {}),
        SubtrialInfo("T2--S2", "2026--F2--L2", "layout-2", {}, {}, {}),
    ]
    monkeypatch.setattr(cron_main, "load_config", lambda require_services=True: app_config)
    monkeypatch.setattr(cron_main, "get_firestore_client", lambda _config: object())
    monkeypatch.setattr(cron_main, "get_storage_client", lambda _config: object())
    monkeypatch.setattr(cron_main, "discover_active_subtrials", lambda _db, _logger: subtrials)
    monkeypatch.setattr(
        cron_main,
        "CloudRunJobClient",
        lambda _config: (_ for _ in ()).throw(
            AssertionError("CloudRunJobClient should not be built")
        ),
    )

    assert cron_main.run(["--step", "discover", "--run-id", "run-1", "--limit-subtrials", "1"]) == 0


# ---------------------------------------------------------------------------
# Preprocessor adapter
# ---------------------------------------------------------------------------


def test_preprocessor_posts_payload_and_reads_selected_shards(monkeypatch, app_config):
    """run_preprocessing posts the raw_prefix payload and reads selected shards
    from both the per-shard subcollection and the legacy `shards` map."""
    http = HttpClient(
        post_body={"batch_run_id": "batch-1"},
        get_body={"status": "success"},
    )
    monkeypatch.setattr(
        "cron_job.services.preprocessor.httpx.Client",
        lambda timeout: http,
    )

    result = run_preprocessing(
        app_config,
        db=FakePreprocessingDb(),
        image_csv_uri="gs://ona-harvest/raw/run-1/trial/subtrial_images.csv",
        batch_run_id="batch-1",
        logger=CaptureLogger(),
        sleep=lambda _seconds: None,
    )

    assert result.success is True
    # Both the per-shard and legacy shard sources are merged and deduplicated.
    assert set(result.selected_shard_uris) == {
        "gs://bucket/selected.csv",
        "gs://bucket/legacy.csv",
    }
    # POST body uses the configured raw_prefix and the supplied batch_run_id.
    posted_url, posted_body = http.posts[0]
    assert posted_url == "http://preprocessor/start"
    assert posted_body["src_bucket"] == "ona-harvest"
    assert posted_body["batch_run_id"] == "batch-1"
    assert "raw_prefix" in posted_body


def test_preprocessor_returns_failure_when_post_returns_4xx(monkeypatch, app_config):
    class FailingHttp(HttpClient):
        def post(self, url, json):
            self.posts.append((url, json))
            return Response({}, status_code=500, text="boom")

    http = FailingHttp()
    monkeypatch.setattr("cron_job.services.preprocessor.httpx.Client", lambda timeout: http)

    result = run_preprocessing(
        app_config,
        db=FakePreprocessingDb(),
        image_csv_uri="gs://ona-harvest/raw/run-1/trial/sub_images.csv",
        batch_run_id="batch-1",
        logger=CaptureLogger(),
        sleep=lambda _seconds: None,
    )

    assert result.success is False
    assert result.status == "failed"
    assert "boom" in (result.error or "")


# ---------------------------------------------------------------------------
# Inference adapter
# ---------------------------------------------------------------------------


def test_inference_submits_async_batch_and_polls_to_output(monkeypatch, app_config):
    """run_inference posts an async batch payload and resolves the output
    folder from the polled completion response."""
    http = HttpClient(
        post_body={"job_id": "job-1"},
        get_body={
            "status": "completed",
            "outputs": {
                "json_folder": "gs://ona-harvest/out/json/",
                "output_folder": "gs://ona-harvest/out/annotated/",
            },
        },
    )
    monkeypatch.setattr(
        "cron_job.services.inference.httpx.Client",
        lambda timeout: http,
    )
    # Use an /images/ prefix so _compute_save_folder produces a usable path
    # rather than hitting the fallback (which has a known undefined-name bug).
    group = ProtocolTraitGroup(
        protocol="RGB",
        raw_trait_value="Pod Count",
        canonical_trait_name="pods",
        inference_trait_type="pod",
        source_document_ids=["image-1"],
        source_collection_paths=["trials/t/subtrials/s/images/p/plot/image-1"],
        image_prefixes=["gs://bucket/x/images/sub/"],
    )

    results = run_inference(
        app_config,
        run_id="run-1",
        subtrial_id="subtrial",
        groups=[group],
        logger=CaptureLogger(),
        sleep=lambda _seconds: None,
    )

    assert len(results) == 1
    assert results[0].success is True
    # output_gcs_path comes from the response's outputs.json_folder.
    assert results[0].output_gcs_path == "gs://ona-harvest/out/json/"
    posted_url, posted_body = http.posts[0]
    assert posted_url == "http://inference/batch"
    assert posted_body["async_processing"] is True
    assert posted_body["trait_type"] == "pod"


def test_inference_returns_failed_when_no_image_prefixes(app_config):
    group = ProtocolTraitGroup(
        protocol="RGB",
        raw_trait_value="Pod Count",
        canonical_trait_name="pods",
        inference_trait_type="pod",
        source_document_ids=["image-1"],
        source_collection_paths=["trials/t/subtrials/s/images/p/plot/image-1"],
        image_prefixes=[],
    )

    results = run_inference(
        app_config,
        run_id="run-1",
        subtrial_id="subtrial",
        groups=[group],
        logger=CaptureLogger(),
        sleep=lambda _seconds: None,
    )

    assert len(results) == 1
    assert results[0].success is False
    assert "no selected image prefixes" in (results[0].error or "")


def test_inference_returns_failed_when_unknown_trait_type(app_config):
    group = ProtocolTraitGroup(
        protocol="RGB",
        raw_trait_value="mystery",
        canonical_trait_name="pods",
        inference_trait_type="mystery_trait",
        source_document_ids=["image-1"],
        source_collection_paths=[],
        image_prefixes=["gs://bucket/x/images/sub/"],
    )

    results = run_inference(
        app_config,
        run_id="run-1",
        subtrial_id="subtrial",
        groups=[group],
        logger=CaptureLogger(),
        sleep=lambda _seconds: None,
    )

    assert results[0].success is False
    assert "no model_id mapping" in (results[0].error or "")


# ---------------------------------------------------------------------------
# CloudRun execution status mapping
# ---------------------------------------------------------------------------


def test_execution_status_maps_terminal_counts():
    status, counts, _start, _complete, _log_uri = _execution_status(
        {
            "taskCount": 2,
            "succeededCount": 1,
            "failedCount": 1,
            "completionTime": "done",
        }
    )

    assert status == "failed"
    assert counts["failedCount"] == 1


def test_execution_status_completed_when_no_failures():
    status, _counts, _start, _complete, _log_uri = _execution_status(
        {
            "taskCount": 1,
            "succeededCount": 1,
            "failedCount": 0,
            "completionTime": "done",
        }
    )
    assert status == "completed"


def test_execution_status_processing_when_started_but_not_complete():
    status, _counts, _start, _complete, _log_uri = _execution_status(
        {"taskCount": 1, "succeededCount": 0, "failedCount": 0, "startTime": "t"}
    )
    assert status == "processing"


def test_execution_status_enqueued_when_no_progress_fields():
    status, _counts, _start, _complete, _log_uri = _execution_status({})
    assert status == "enqueued"


# ---------------------------------------------------------------------------
# run() exit codes
# ---------------------------------------------------------------------------


def test_run_returns_failure_when_any_subtrial_fails(monkeypatch, app_config):
    subtrial = SubtrialInfo("T--S", "2026--F--L", "layout", {}, {}, {})
    failed_state = SubtrialState("T--S", "2026--F--L", 1, "batch-1", status="failed")
    monkeypatch.setattr(cron_main, "load_config", lambda require_services=True: app_config)
    monkeypatch.setattr(cron_main, "get_firestore_client", lambda _config: object())
    monkeypatch.setattr(cron_main, "get_storage_client", lambda _config: object())
    monkeypatch.setattr(cron_main, "CloudRunJobClient", lambda _config: object())
    monkeypatch.setattr(cron_main, "discover_active_subtrials", lambda _db, _logger: [subtrial])
    monkeypatch.setattr(cron_main, "process_subtrial", lambda *_args, **_kwargs: failed_state)

    assert cron_main.run(["--run-id", "run-1", "--run-date", "2026-05-14"]) == 1


def test_run_returns_success_with_no_active_layouts(monkeypatch, app_config):
    monkeypatch.setattr(cron_main, "load_config", lambda require_services=True: app_config)
    monkeypatch.setattr(cron_main, "get_firestore_client", lambda _config: object())
    monkeypatch.setattr(cron_main, "get_storage_client", lambda _config: object())
    monkeypatch.setattr(cron_main, "CloudRunJobClient", lambda _config: object())
    monkeypatch.setattr(cron_main, "discover_active_subtrials", lambda _db, _logger: [])

    assert cron_main.run(["--run-id", "run-1", "--run-date", "2026-05-14"]) == 0


def test_run_returns_success_when_subtrial_succeeds(monkeypatch, app_config):
    subtrial = SubtrialInfo("T--S", "2026--F--L", "layout", {}, {}, {})
    ok_state = SubtrialState("T--S", "2026--F--L", 1, "batch-1", status="succeeded")
    monkeypatch.setattr(cron_main, "load_config", lambda require_services=True: app_config)
    monkeypatch.setattr(cron_main, "get_firestore_client", lambda _config: object())
    monkeypatch.setattr(cron_main, "get_storage_client", lambda _config: object())
    monkeypatch.setattr(cron_main, "CloudRunJobClient", lambda _config: object())
    monkeypatch.setattr(cron_main, "discover_active_subtrials", lambda _db, _logger: [subtrial])
    monkeypatch.setattr(cron_main, "process_subtrial", lambda *_args, **_kwargs: ok_state)

    assert cron_main.run(["--run-id", "run-1", "--run-date", "2026-05-14"]) == 0


def test_multi_date_route_skips_subtrial_discovery(monkeypatch, app_config):
    """When --data-collected-dates is supplied, run() loads a single subtrial
    and calls _process_multi_date instead of the discovery loop."""
    captured: dict = {}

    def fake_load(_db, trial_id, subtrial_id):
        return SubtrialInfo(trial_id, subtrial_id, "layout", {}, {}, {})

    def fake_multi(_config, **kwargs):
        captured["dates"] = [d.isoformat() for d in kwargs["dates"]]
        captured["subtrial_id"] = kwargs["subtrial"].subtrial_id
        return SubtrialState(
            kwargs["subtrial"].trial_id,
            kwargs["subtrial"].subtrial_id,
            0,
            "batch-1",
            status="succeeded",
        )

    monkeypatch.setattr(cron_main, "load_config", lambda require_services=True: app_config)
    monkeypatch.setattr(cron_main, "get_firestore_client", lambda _config: object())
    monkeypatch.setattr(cron_main, "get_storage_client", lambda _config: object())
    monkeypatch.setattr(cron_main, "CloudRunJobClient", lambda _config: object())
    monkeypatch.setattr(cron_main, "_load_subtrial_info", fake_load)
    monkeypatch.setattr(cron_main, "_process_multi_date", fake_multi)
    monkeypatch.setattr(
        cron_main,
        "discover_active_subtrials",
        lambda *_a, **_kw: (_ for _ in ()).throw(AssertionError("discovery should not run")),
    )

    rc = cron_main.run(
        [
            "--run-id",
            "run-1",
            "--data-collected-dates",
            "2026-05-14+2026-05-15",
            "--trial-id",
            "T--S",
            "--subtrial-id",
            "2026--F--L",
        ]
    )

    assert rc == 0
    assert captured["dates"] == ["2026-05-14", "2026-05-15"]
    assert captured["subtrial_id"] == "2026--F--L"
