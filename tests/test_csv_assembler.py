"""Tests for services/csv/assembler.py internal helpers.

Focuses on:
- _classical_phenotyping_path: requires all six metadata fields to be populated
- _image_uri_from_fields: bucket_prefix + gcs_img_path combination logic
- _meta_image_uuid: stem extraction with image_uuid / document_id fallbacks
"""
from __future__ import annotations

from cron_job.services.csv import assembler

from .conftest import make_classical_doc


def test_classical_phenotyping_path_uses_first_doc_with_all_fields_populated():
    docs = [
        make_classical_doc("doc-1"),  # all 6 fields populated by default
    ]
    path = assembler._classical_phenotyping_path(docs, run_id="run-1", run_date="2026-05-14")
    assert path is not None
    assert path.startswith("Artemis/Arusha--CIAT/mvp-validation/")
    assert path.endswith(".csv")
    assert "trait_collection/classical-phenotyping/" in path


def test_classical_phenotyping_path_returns_none_when_metadata_fields_missing():
    docs = [
        make_classical_doc(
            "doc-1",
            extra_fields={"project_name": "", "site_name": ""},
        )
    ]
    # Patch the doc to actually have empty metadata fields.
    docs[0].fields["project_name"] = ""
    docs[0].fields["site_name"] = ""
    path = assembler._classical_phenotyping_path(docs, run_id="run-1", run_date="2026-05-14")
    assert path is None


def test_classical_phenotyping_path_skips_first_doc_with_blanks_uses_next():
    bad_doc = make_classical_doc("doc-1")
    bad_doc.fields["project_name"] = ""
    good_doc = make_classical_doc("doc-2")

    path = assembler._classical_phenotyping_path(
        [bad_doc, good_doc], run_id="run-1", run_date="2026-05-14"
    )
    assert path is not None
    # Path is built from good_doc's fields.
    assert path.startswith("Artemis/")


def test_image_uri_from_fields_returns_full_uri_when_gcs_img_path_already_gs():
    fields = {"gcs_img_path": "gs://bucket/full/path.jpg"}
    assert assembler._image_uri_from_fields(fields) == "gs://bucket/full/path.jpg"


def test_image_uri_from_fields_combines_bucket_prefix_and_relative_path():
    fields = {"bucket_prefix": "ona-harvest", "gcs_img_path": "a/b/img.jpg"}
    assert assembler._image_uri_from_fields(fields) == "gs://ona-harvest/a/b/img.jpg"


def test_image_uri_from_fields_handles_gs_prefix_in_bucket_prefix():
    fields = {"bucket_prefix": "gs://ona-harvest/", "gcs_img_path": "a/b/img.jpg"}
    assert assembler._image_uri_from_fields(fields) == "gs://ona-harvest/a/b/img.jpg"


def test_image_uri_from_fields_returns_empty_when_gcs_img_path_missing():
    assert assembler._image_uri_from_fields({}) == ""
    assert assembler._image_uri_from_fields({"bucket_prefix": "ona-harvest"}) == ""


def test_meta_image_uuid_uses_gcs_img_path_stem():
    fields = {"gcs_img_path": "a/b/abc-123.jpg"}
    assert assembler._meta_image_uuid(fields, "doc-id") == "abc-123"


def test_meta_image_uuid_falls_back_to_image_uuid_field():
    fields = {"image_uuid": "fallback-uuid"}
    assert assembler._meta_image_uuid(fields, "doc-id") == "fallback-uuid"


def test_meta_image_uuid_falls_back_to_document_id():
    assert assembler._meta_image_uuid({}, "doc-id-final") == "doc-id-final"
