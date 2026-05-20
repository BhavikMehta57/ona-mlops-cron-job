"""Tests for services/firestore/scanner.py internal helpers.

The end-to-end scan_subtrial_documents flow is covered in
test_daily_pipeline_units.py. This file pins the date-filter helpers and the
UTC day window math.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from cron_job.services.firestore import scanner


# ---------------------------------------------------------------------------
# UTC window
# ---------------------------------------------------------------------------


def test_utc_day_window_is_inclusive_start_exclusive_end():
    start, end = scanner.utc_day_window(date(2026, 5, 14))
    assert start == datetime(2026, 5, 14, 0, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 5, 15, 0, 0, 0, tzinfo=timezone.utc)
    assert end - start == timedelta(days=1)


# ---------------------------------------------------------------------------
# _is_in_window
# ---------------------------------------------------------------------------


def test_is_in_window_treats_naive_datetime_as_utc():
    start = datetime(2026, 5, 14, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    naive = datetime(2026, 5, 14, 10, 30)  # no tzinfo

    assert scanner._is_in_window(naive, start, end) is True


def test_is_in_window_excludes_end_boundary():
    start = datetime(2026, 5, 14, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    assert scanner._is_in_window(end, start, end) is False
    assert scanner._is_in_window(start, start, end) is True


def test_is_in_window_returns_false_for_none_or_unsupported_value():
    start = datetime(2026, 5, 14, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    assert scanner._is_in_window(None, start, end) is False
    assert scanner._is_in_window("2026-05-14", start, end) is False


# ---------------------------------------------------------------------------
# _matches_data_collection_date (the data_collection string filter)
# ---------------------------------------------------------------------------


def test_matches_data_collection_date_matches_iso_prefix():
    assert scanner._matches_data_collection_date(
        {"data_collection": "2026-05-14T08:30:00"}, "2026-05-14"
    ) is True


def test_matches_data_collection_date_excludes_other_dates():
    assert scanner._matches_data_collection_date(
        {"data_collection": "2026-05-15"}, "2026-05-14"
    ) is False


def test_matches_data_collection_date_returns_false_when_field_missing():
    assert scanner._matches_data_collection_date({}, "2026-05-14") is False


def test_matches_data_collection_date_strips_whitespace():
    assert scanner._matches_data_collection_date(
        {"data_collection": "  2026-05-14T08:30:00  "}, "2026-05-14"
    ) is True


# ---------------------------------------------------------------------------
# Identity helpers used during discovery
# ---------------------------------------------------------------------------


def test_make_trial_id_concatenates_trial_and_site_with_double_dash():
    assert scanner._make_trial_id({"trial_name": "my-trial", "site_name": "my-site"}) == "my-trial--my-site"


def test_make_trial_id_returns_none_when_either_part_missing():
    assert scanner._make_trial_id({"trial_name": "T"}) is None
    assert scanner._make_trial_id({"site_name": "S"}) is None
    assert scanner._make_trial_id({"trial_name": "  ", "site_name": "S"}) is None


def test_make_subtrial_id_concatenates_season_field_location():
    layout = {"season": "2026", "field": "F", "location": "L"}
    assert scanner._make_subtrial_id(layout) == "2026--F--L"


def test_make_subtrial_id_returns_none_when_any_part_missing():
    assert scanner._make_subtrial_id({"season": "2026", "field": "F"}) is None
    assert scanner._make_subtrial_id({"season": "", "field": "F", "location": "L"}) is None
