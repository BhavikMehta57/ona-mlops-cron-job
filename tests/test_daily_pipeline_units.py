"""Unit tests for cron_job pipeline modules.

Covers:
- CSV assembly (deterministic header, leading columns, field-level row preservation)
- Protocol/trait group resolution
- Firestore discovery, dedup, and skip-on-missing-hierarchy
- Firestore scanning with both upload-timestamp and data_collection filter modes
- process_subtrial skipped behaviour
- CloudRunJobClient run_job_with_inline_spec body shape
"""
from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timedelta, timezone

from cron_job.main import process_subtrial
from cron_job.schemas.models import (
    RunContext,
    ScannedDocument,
    SubtrialDocuments,
    SubtrialInfo,
)
from cron_job.services.csv.assembler import (
    IMAGE_COMPAT_COLUMNS,
    LEADING_COLUMNS,
    assemble_csv,
)
from cron_job.services.firestore.scanner import (
    discover_active_subtrials,
    scan_subtrial_documents,
)
from cron_job.services.trait_extractor import CloudRunJobClient, CloudRunJobRef
from cron_job.services.utils.protocol_trait import (
    canonical_trait_from_value,
    group_documents_by_protocol_trait,
)

from .conftest import CaptureLogger, make_image_doc


# ---------------------------------------------------------------------------
# Firestore fakes (kept module-local; real db is never called)
# ---------------------------------------------------------------------------


class FakeSnapshot:
    def __init__(self, doc_id, data=None, *, exists=True, path=None):
        self.id = doc_id
        self._data = data or {}
        self.exists = exists
        self.reference = type("Ref", (), {"path": path or doc_id})()

    def to_dict(self):
        return dict(self._data)


class FakeQuery:
    def __init__(self, snaps):
        self._snaps = list(snaps)

    def where(self, *_args):
        return self

    def stream(self):
        return list(self._snaps)


class FakeCollection(FakeQuery):
    def __init__(self, snaps=None, documents=None):
        super().__init__(snaps or [])
        self._documents = documents or {}

    def document(self, doc_id):
        return self._documents[doc_id]

    def list_documents(self, page_size=None):
        return list(self._documents.values())


class FakeDocumentRef:
    def __init__(self, doc_id, data=None, *, exists=True, path=None, collections=None):
        self.id = doc_id
        self._snapshot = FakeSnapshot(doc_id, data, exists=exists, path=path or doc_id)
        self._collections = collections or {}
        self.path = path or doc_id

    def get(self):
        return self._snapshot

    def collection(self, name):
        return self._collections.get(name, FakeCollection())

    def collections(self):
        return list(self._collections.values())


class FakeDb:
    def __init__(self, *, layouts, trials):
        self._layouts = layouts
        self._trials = trials

    def collection(self, name):
        if name == "trial_layouts":
            return FakeCollection(self._layouts)
        if name == "trials":
            return FakeCollection(documents=self._trials)
        raise AssertionError(name)


# ---------------------------------------------------------------------------
# CSV assembly
# ---------------------------------------------------------------------------


def test_image_csv_has_canonical_and_e2e_columns():
    text = assemble_csv([make_image_doc()], csv_type="images")
    reader = csv.DictReader(io.StringIO(text))

    assert reader.fieldnames[:8] == LEADING_COLUMNS + IMAGE_COMPAT_COLUMNS
    row = next(reader)
    assert row["image_uri"] == "gs://ona-harvest/a/b/image-uuid.jpg"
    assert row["img_bucket_prefix"] == "ona-harvest"
    assert row["img_gcs_img_path"] == "a/b/image-uuid.jpg"
    assert row["meta_image_uuid"] == "image-uuid"


def test_assemble_csv_returns_empty_string_for_no_documents():
    assert assemble_csv([], csv_type="images") == ""


def test_assemble_csv_classical_does_not_include_image_compat_columns():
    doc = make_image_doc()
    classical_doc = ScannedDocument(
        collection_name="flowering_data",
        document_id=doc.document_id,
        collection_path=doc.collection_path,
        protocol_date_id=None,
        image_uri=None,
        protocol="manual",
        trait="Flowering Date",
        upload_timestamp=doc.upload_timestamp,
        fields={"plot_uid": "plot-1", "score": "5"},
    )
    text = assemble_csv([classical_doc], csv_type="classical")
    reader = csv.DictReader(io.StringIO(text))

    assert reader.fieldnames[:5] == LEADING_COLUMNS
    assert "img_bucket_prefix" not in reader.fieldnames
    assert "img_gcs_img_path" not in reader.fieldnames


def test_assemble_csv_dynamic_columns_are_sorted():
    a = make_image_doc("a", extra_fields={"zebra": "z", "apple": "a"})
    b = make_image_doc("b", extra_fields={"mango": "m"})
    text = assemble_csv([a, b], csv_type="images")
    columns = next(csv.reader(io.StringIO(text)))
    dynamic = [c for c in columns if c not in LEADING_COLUMNS + IMAGE_COMPAT_COLUMNS]

    assert dynamic == sorted(dynamic)


# ---------------------------------------------------------------------------
# Protocol-trait resolution
# ---------------------------------------------------------------------------


def test_canonical_trait_from_value_keyword_matching():
    assert canonical_trait_from_value("Pod Count") == "pods"
    assert canonical_trait_from_value("Flowering Date") == "flowering"
    assert canonical_trait_from_value("Plant Stand") == "plantstand"
    assert canonical_trait_from_value("unknown trait") is None
    assert canonical_trait_from_value(None) is None
    assert canonical_trait_from_value("") is None


def test_protocol_trait_groups_selected_prefixes():
    logger = CaptureLogger()
    docs = [
        make_image_doc("a"),
        make_image_doc("b"),
        make_image_doc("c", trait="Flowering Date"),
    ]
    prefixes = {
        "a": "gs://bucket/pod-a/",
        "b": "gs://bucket/pod-a/",
        "c": "gs://bucket/flower/",
    }

    groups = group_documents_by_protocol_trait(
        docs, logger, image_prefixes_by_document_id=prefixes
    )

    by_canonical = {g.canonical_trait_name: g for g in groups}
    assert set(by_canonical) == {"pods", "flowering"}
    assert by_canonical["pods"].image_prefixes == ["gs://bucket/pod-a/"]
    assert by_canonical["flowering"].image_prefixes == ["gs://bucket/flower/"]


def test_protocol_trait_groups_skips_docs_with_unknown_trait():
    logger = CaptureLogger()
    docs = [
        make_image_doc("a", trait="UnknownTraitValue"),
        make_image_doc("b", trait="Pod Count"),
    ]
    groups = group_documents_by_protocol_trait(docs, logger)

    assert len(groups) == 1
    assert groups[0].canonical_trait_name == "pods"
    warnings = logger.find(stage="protocol_trait_resolution", status="warning")
    assert any("unrecognized trait" in str(e.get("errors", "")) for e in warnings)


def test_protocol_trait_groups_skips_docs_with_missing_protocol():
    logger = CaptureLogger()
    docs = [make_image_doc("a", protocol="")]
    groups = group_documents_by_protocol_trait(docs, logger)

    assert groups == []
    warnings = logger.find(stage="protocol_trait_resolution", status="warning")
    assert any("missing protocol" in str(e.get("errors", "")) for e in warnings)


# ---------------------------------------------------------------------------
# Firestore discovery
# ---------------------------------------------------------------------------


def test_discovery_uses_active_layouts_dedupes_and_skips_missing_hierarchy():
    layouts = [
        FakeSnapshot(
            "layout-1",
            {
                "isActive": True,
                "trial_name": "T",
                "site_name": "S",
                "season": "2026",
                "field": "F",
                "location": "L",
            },
        ),
        FakeSnapshot(
            "layout-2",
            {
                "isActive": True,
                "trial_name": "T",
                "site_name": "S",
                "season": "2026",
                "field": "F",
                "location": "L",
            },
        ),
        FakeSnapshot(
            "layout-3",
            {
                "isActive": True,
                "trial_name": "Missing",
                "site_name": "S",
                "season": "2026",
                "field": "F",
                "location": "L",
            },
        ),
    ]
    subtrial = FakeDocumentRef(
        "2026--F--L",
        {"planting_date": "2026-04-01"},
        path="trials/T--S/subtrials/2026--F--L",
    )
    trial = FakeDocumentRef(
        "T--S",
        {"name": "trial"},
        path="trials/T--S",
        collections={
            "subtrials": FakeCollection(documents={"2026--F--L": subtrial})
        },
    )
    missing_trial = FakeDocumentRef("Missing--S", exists=False)
    db = FakeDb(layouts=layouts, trials={"T--S": trial, "Missing--S": missing_trial})
    logger = CaptureLogger()

    discovered = discover_active_subtrials(db, logger)

    assert [(s.trial_id, s.subtrial_id) for s in discovered] == [("T--S", "2026--F--L")]
    assert any(
        e["stage"] == "discovery" and e["status"] == "skipped" for e in logger.entries
    )


def test_discovery_logs_no_active_trials_when_layouts_empty():
    db = FakeDb(layouts=[], trials={})
    logger = CaptureLogger()

    discovered = discover_active_subtrials(db, logger)

    assert discovered == []
    assert any(e["status"] == "no_active_trials" for e in logger.entries)


# ---------------------------------------------------------------------------
# Firestore scanning
# ---------------------------------------------------------------------------


def test_scan_subtrial_documents_walks_image_plot_subcollections_and_filters_utc_day():
    day = datetime(2026, 5, 14, tzinfo=timezone.utc)
    in_window = FakeSnapshot(
        "image-1",
        {
            "upload_timestamp": day + timedelta(hours=4),
            "gcs_img_path": "x/image-1.jpg",
            "bucket_prefix": "ona-harvest",
        },
        path="trials/T--S/subtrials/2026--F--L/images/RGB--2026-05-14/plot-1/image-1",
    )
    out_window = FakeSnapshot(
        "image-2",
        {
            "upload_timestamp": day + timedelta(days=1),
            "gcs_img_path": "x/image-2.jpg",
            "bucket_prefix": "ona-harvest",
        },
        path="trials/T--S/subtrials/2026--F--L/images/RGB--2026-05-14/plot-1/image-2",
    )
    plot_collection = FakeCollection([in_window, out_window])
    plot_collection.id = "plot-1"
    protocol_doc = FakeDocumentRef(
        "RGB--2026-05-14",
        {"protocol": "RGB", "trait": "Pod Count"},
        collections={"plot-1": plot_collection},
    )
    flowering = FakeSnapshot(
        "flower-1",
        {
            "upload_timestamp": day + timedelta(hours=2),
            "protocol": "manual",
            "trait": "Flowering Date",
        },
        path="trials/T--S/subtrials/2026--F--L/flowering_data/flower-1",
    )
    subtrial_doc = FakeDocumentRef(
        "2026--F--L",
        collections={
            "images": FakeCollection(documents={"RGB--2026-05-14": protocol_doc}),
            "two_images_with_count": FakeCollection(documents={"p": FakeDocumentRef("p")}),
            "flowering_data": FakeCollection([flowering]),
            "numeric_data": FakeCollection(),
        },
    )
    trial = FakeDocumentRef(
        "T--S",
        collections={"subtrials": FakeCollection(documents={"2026--F--L": subtrial_doc})},
    )
    db = FakeDb(layouts=[], trials={"T--S": trial})
    subtrial = SubtrialInfo("T--S", "2026--F--L", "layout", {}, {}, {})
    logger = CaptureLogger()

    docs = scan_subtrial_documents(db, subtrial, day.date(), logger)

    assert [doc.document_id for doc in docs.image_documents] == ["image-1"]
    assert [doc.document_id for doc in docs.classical_documents] == ["flower-1"]
    assert docs.skipped_two_images_count == 1
    assert any(
        e["collection_name"] == "two_images_with_count" for e in logger.entries
    )


def test_scan_subtrial_documents_filters_by_data_collection_string_prefix():
    day = datetime(2026, 5, 14, tzinfo=timezone.utc)
    matching = FakeSnapshot(
        "doc-1",
        {
            "data_collection": "2026-05-14T08:30:00",
            "protocol": "manual",
            "trait": "Flowering Date",
        },
        path="trials/T--S/subtrials/2026--F--L/flowering_data/doc-1",
    )
    other_day = FakeSnapshot(
        "doc-2",
        {
            "data_collection": "2026-05-15T08:30:00",
            "protocol": "manual",
            "trait": "Flowering Date",
        },
        path="trials/T--S/subtrials/2026--F--L/flowering_data/doc-2",
    )
    subtrial_doc = FakeDocumentRef(
        "2026--F--L",
        collections={
            "images": FakeCollection(),
            "two_images_with_count": FakeCollection(),
            "flowering_data": FakeCollection([matching, other_day]),
            "numeric_data": FakeCollection(),
        },
    )
    trial = FakeDocumentRef(
        "T--S",
        collections={"subtrials": FakeCollection(documents={"2026--F--L": subtrial_doc})},
    )
    db = FakeDb(layouts=[], trials={"T--S": trial})
    subtrial = SubtrialInfo("T--S", "2026--F--L", "layout", {}, {}, {})
    logger = CaptureLogger()

    docs = scan_subtrial_documents(
        db, subtrial, day.date(), logger, data_collected_date="2026-05-14"
    )

    assert [d.document_id for d in docs.classical_documents] == ["doc-1"]


# ---------------------------------------------------------------------------
# process_subtrial behaviour
# ---------------------------------------------------------------------------


def test_process_subtrial_skips_when_no_documents(monkeypatch, app_config):
    """When scan returns no documents, the subtrial is marked skipped."""
    subtrial = SubtrialInfo("T--S", "2026--F--L", "layout", {}, {}, {})
    context = RunContext(
        "run-1",
        datetime(2026, 5, 14, tzinfo=timezone.utc).date(),
        datetime.now(timezone.utc),
    )
    # Patch with **kwargs so the data_collected_date keyword does not cause TypeError.
    monkeypatch.setattr(
        "cron_job.main.scan_subtrial_documents",
        lambda *_args, **_kwargs: SubtrialDocuments(),
    )

    state = process_subtrial(
        app_config,
        db=object(),
        storage_client=object(),
        cloud_run_client=object(),
        context=context,
        subtrial=subtrial,
        index=1,
        logger=CaptureLogger(),
    )

    assert state.status == "skipped"
    assert state.cv_path_status == "not_applicable"
    assert state.classical_path_status == "not_applicable"


def test_process_subtrial_marks_failed_when_scan_errors_present(monkeypatch, app_config):
    """A subtrial with scan_errors and no docs returned is failed, not skipped."""
    subtrial = SubtrialInfo("T--S", "2026--F--L", "layout", {}, {}, {})
    context = RunContext(
        "run-1",
        datetime(2026, 5, 14, tzinfo=timezone.utc).date(),
        datetime.now(timezone.utc),
    )
    docs = SubtrialDocuments(scan_errors=["images: boom"])
    monkeypatch.setattr(
        "cron_job.main.scan_subtrial_documents",
        lambda *_args, **_kwargs: docs,
    )

    state = process_subtrial(
        app_config,
        db=object(),
        storage_client=object(),
        cloud_run_client=object(),
        context=context,
        subtrial=subtrial,
        index=1,
        logger=CaptureLogger(),
    )

    assert state.status == "failed"
    assert state.failed_stage == "scanning"


# ---------------------------------------------------------------------------
# CloudRunJobClient inline run-spec request shape
# ---------------------------------------------------------------------------


def test_cloud_run_job_payload_uses_inline_run_spec():
    class Response:
        status_code = 200
        text = "{}"

        def raise_for_status(self):
            return None

        def json(self):
            return {"name": "operations/op-1"}

    class Http:
        def __init__(self):
            self.body = None

        def post(self, _url, headers, json):
            self.body = json
            return Response()

    http = Http()
    client = object.__new__(CloudRunJobClient)
    client.ref = CloudRunJobRef("project", "us-central1", "ona-trait-extraction")
    client._base = "https://run.googleapis.com/v2"
    client._http = http
    client._headers = lambda: {"Authorization": "Bearer token"}

    client.run_job_with_inline_spec(
        {
            "trait": "pods",
            "method": "computer_vision",
            "input_dir": "gs://b/in",
            "output_prefix": "gs://b/out",
        }
    )

    args = http.body["overrides"]["containerOverrides"][0]["args"]
    assert args[:3] == ["-m", "trait_extraction.runner.entrypoint", "--run-spec-json"]
    assert json.loads(args[3])["method"] == "computer_vision"
