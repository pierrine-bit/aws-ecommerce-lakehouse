import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

GLUE_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "glue_scripts"
sys.path.insert(0, str(GLUE_SCRIPTS_DIR))

import lakehouse_delta_etl as etl  # noqa: E402


@pytest.fixture(scope="module")
def spark_session():
    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder
            .master("local[1]")
            .appName("lakehouse-delta-etl-tests")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"Local Spark session unavailable (likely missing Java): {exc}")

    yield session
    session.stop()


def test_clean_excel_dataframe_coerces_types_and_strips_columns():
    import pandas as pd

    raw = pd.DataFrame({
        " order_id ": ["1", "2", "x"],
        "total_amount": ["9.99", None, "-5.00"],
        "order_timestamp": ["2025-04-01 10:00:00", "not-a-date", None],
    })

    cleaned = etl.clean_excel_dataframe(raw)

    assert list(cleaned.columns) == ["order_id", "total_amount", "order_timestamp"]
    # order_id is coerced to a nullable long: non-numeric values become None.
    assert cleaned["order_id"].tolist() == [1, 2, None]
    # total_amount is coerced to a nullable double.
    assert cleaned["total_amount"].tolist() == [9.99, None, -5.0]
    # Unparseable/missing timestamps must read as NA (representation varies
    # by pandas version - None/NaN/NaT are all acceptable) rather than
    # crashing or silently becoming a valid-looking value.
    assert pd.isna(cleaned["order_timestamp"].iloc[1])
    assert pd.isna(cleaned["order_timestamp"].iloc[2])


def test_validate_required_columns_raises_on_missing(spark_session):
    df = spark_session.createDataFrame([(1, "a")], ["id", "name"])
    with pytest.raises(ValueError):
        etl.validate_required_columns(df, "orders", ["id", "name", "total_amount"])


def test_validate_required_columns_passes_when_present(spark_session):
    df = spark_session.createDataFrame([(1, "a")], ["id", "name"])
    etl.validate_required_columns(df, "orders", ["id", "name"])


def test_split_valid_invalid_rejects_null_id_columns(spark_session):
    df = spark_session.createDataFrame(
        [(1, "2025-04-01T00:00:00"), (None, "2025-04-01T00:00:00")],
        ["order_id", "order_timestamp"],
    )

    valid, invalid = etl.split_valid_invalid(df, ["order_id"])

    assert valid.count() == 1
    assert invalid.count() == 1


def test_split_valid_invalid_rejects_unparseable_timestamp(spark_session):
    df = spark_session.createDataFrame(
        [(1, "2025-04-01T00:00:00"), (2, "not-a-timestamp")],
        ["order_id", "order_timestamp"],
    )

    valid, invalid = etl.split_valid_invalid(df, ["order_id"], timestamp_column="order_timestamp")

    assert valid.count() == 1
    assert invalid.count() == 1


def test_orders_business_rule_rejects_negative_or_null_total_amount(spark_session):
    """Mirrors the exact rule process_orders() applies in lakehouse_delta_etl.py."""
    from pyspark.sql.functions import col

    df = spark_session.createDataFrame(
        [(1, 9.99), (2, -0.01), (3, None)],
        ["order_id", "total_amount"],
    )
    negative_amount = col("total_amount").isNull() | (col("total_amount") < 0)

    valid, invalid = etl.split_valid_invalid(
        df, ["order_id"], business_rule_condition=negative_amount
    )

    assert [row.order_id for row in valid.collect()] == [1]
    assert sorted(row.order_id for row in invalid.collect()) == [2, 3]


def test_order_items_business_rule_allows_null_but_rejects_negative_days(spark_session):
    """Mirrors the exact rule process_order_items() applies in lakehouse_delta_etl.py."""
    from pyspark.sql.functions import col

    df = spark_session.createDataFrame(
        [(1, None), (2, 5.0), (3, -1.0)],
        ["id", "days_since_prior_order"],
    )
    negative_days_since_prior_order = col("days_since_prior_order") < 0

    valid, invalid = etl.split_valid_invalid(
        df, ["id"], business_rule_condition=negative_days_since_prior_order
    )

    # A null days_since_prior_order (first-ever order) is valid; only the
    # negative value is rejected.
    assert sorted(row.id for row in valid.collect()) == [1, 2]
    assert [row.id for row in invalid.collect()] == [3]
