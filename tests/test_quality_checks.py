import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

GLUE_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "glue_scripts"
sys.path.insert(0, str(GLUE_SCRIPTS_DIR))

import quality_checks as qc  # noqa: E402


class FakeEntityNotFoundException(Exception):
    pass


def make_s3(commit_timestamps_by_table):
    """A fake S3 client whose list_objects_v2 paginator returns one
    _delta_log object per given timestamp, keyed by table."""
    s3 = MagicMock()

    def get_paginator(_name):
        paginator = MagicMock()

        def paginate(Bucket, Prefix):
            for table, timestamps in commit_timestamps_by_table.items():
                if Prefix == f"processed/{table}/_delta_log/":
                    yield {
                        "Contents": [
                            {"Key": f"{Prefix}{i:020d}.json", "LastModified": ts}
                            for i, ts in enumerate(timestamps)
                        ]
                    }
                    return
            yield {}

        paginator.paginate.side_effect = paginate
        return paginator

    s3.get_paginator.side_effect = get_paginator
    return s3


def make_glue(missing_tables=()):
    glue = MagicMock()
    glue.exceptions.EntityNotFoundException = FakeEntityNotFoundException

    def get_table(DatabaseName, Name):
        if Name in missing_tables:
            raise FakeEntityNotFoundException(Name)
        return {"Table": {"Name": Name}}

    glue.get_table.side_effect = get_table
    return glue


def test_evaluate_tables_all_healthy():
    now = datetime(2026, 7, 31, tzinfo=timezone.utc)
    fresh = now - timedelta(hours=2)
    s3 = make_s3({t: [fresh] for t in qc.REQUIRED_TABLES})
    glue = make_glue()

    missing_delta_log, missing_catalog_entry, stale_tables = qc.evaluate_tables(
        s3, glue, "bucket", "processed", "db", max_data_age_hours=24, now=now
    )

    assert missing_delta_log == []
    assert missing_catalog_entry == []
    assert stale_tables == []


def test_evaluate_tables_flags_missing_delta_log():
    now = datetime(2026, 7, 31, tzinfo=timezone.utc)
    fresh = now - timedelta(hours=1)
    # "orders" has no _delta_log objects at all.
    s3 = make_s3({"products": [fresh], "order_items": [fresh]})
    glue = make_glue()

    missing_delta_log, _, _ = qc.evaluate_tables(
        s3, glue, "bucket", "processed", "db", max_data_age_hours=24, now=now
    )

    assert missing_delta_log == ["orders"]


def test_evaluate_tables_flags_missing_catalog_entry():
    now = datetime(2026, 7, 31, tzinfo=timezone.utc)
    fresh = now - timedelta(hours=1)
    s3 = make_s3({t: [fresh] for t in qc.REQUIRED_TABLES})
    glue = make_glue(missing_tables=["order_items"])

    _, missing_catalog_entry, _ = qc.evaluate_tables(
        s3, glue, "bucket", "processed", "db", max_data_age_hours=24, now=now
    )

    assert missing_catalog_entry == ["order_items"]


def test_evaluate_tables_flags_stale_delta_log():
    now = datetime(2026, 7, 31, tzinfo=timezone.utc)
    fresh = now - timedelta(hours=1)
    stale = now - timedelta(hours=48)
    s3 = make_s3({"products": [fresh], "orders": [stale], "order_items": [fresh]})
    glue = make_glue()

    _, _, stale_tables = qc.evaluate_tables(
        s3, glue, "bucket", "processed", "db", max_data_age_hours=24, now=now
    )

    assert stale_tables == ["orders"]


def test_evaluate_tables_uses_latest_commit_not_first():
    """A table with an old first commit and a recent one should read as fresh."""
    now = datetime(2026, 7, 31, tzinfo=timezone.utc)
    old = now - timedelta(hours=100)
    recent = now - timedelta(hours=1)
    s3 = make_s3({t: [old, recent] for t in qc.REQUIRED_TABLES})
    glue = make_glue()

    _, _, stale_tables = qc.evaluate_tables(
        s3, glue, "bucket", "processed", "db", max_data_age_hours=24, now=now
    )

    assert stale_tables == []


def test_run_quality_gate_raises_on_stale_table():
    now = datetime(2026, 7, 31, tzinfo=timezone.utc)
    stale = now - timedelta(hours=48)
    s3 = make_s3({t: [stale] for t in qc.REQUIRED_TABLES})
    glue = make_glue()

    with pytest.raises(RuntimeError, match="have not been updated"):
        qc.run_quality_gate(s3, glue, "bucket", "processed", "db", max_data_age_hours=24, now=now)


def test_run_quality_gate_passes_when_healthy():
    now = datetime(2026, 7, 31, tzinfo=timezone.utc)
    fresh = now - timedelta(hours=1)
    s3 = make_s3({t: [fresh] for t in qc.REQUIRED_TABLES})
    glue = make_glue()

    result = qc.run_quality_gate(s3, glue, "bucket", "processed", "db", max_data_age_hours=24, now=now)

    assert result == qc.REQUIRED_TABLES
