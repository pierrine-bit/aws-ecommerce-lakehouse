import logging
import sys
import os
import tempfile

import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("ecommerce-lakehouse-delta-etl")

# Fix pandas 2.x compatibility issue with PySpark on Glue 4.0
if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items

# Populated by bootstrap() when this module is run as the Glue job entry
# point. Left as None on import so the transformation/validation functions
# below can be unit tested without a live Glue job context or Spark session.
bucket = database = raw_prefix = processed_prefix = rejected_prefix = None
spark = None
s3 = None


def bootstrap():
    """Resolve Glue job arguments and start the Spark + Delta Lake session.

    Kept separate from module import so tests can exercise the pure
    transformation/validation functions without needing the awsglue
    package, which is only available inside an actual Glue job.
    """
    global bucket, database, raw_prefix, processed_prefix, rejected_prefix, spark, s3
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(
        sys.argv,
        ["bucket", "database", "raw_prefix", "processed_prefix", "rejected_prefix"],
    )

    bucket = args["bucket"]
    database = args["database"]
    raw_prefix = args["raw_prefix"].strip("/")
    processed_prefix = args["processed_prefix"].strip("/")
    rejected_prefix = args["rejected_prefix"].strip("/")

    spark = (
        SparkSession.builder
        .appName("ecommerce-lakehouse-delta-etl")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
        .getOrCreate()
    )

    s3 = boto3.client("s3")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")


PRODUCT_SCHEMA = StructType([
    StructField("product_id", LongType(), True),
    StructField("department_id", LongType(), True),
    StructField("department", StringType(), True),
    StructField("product_name", StringType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_num", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("date", StringType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("days_since_prior_order", DoubleType(), True),
    StructField("product_id", LongType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", IntegerType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("date", StringType(), True),
])


def s3_path(key):
    return f"s3://{bucket}/{key}"


def download_s3_object(key):
    suffix = os.path.splitext(key)[1]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.close()
    s3.download_file(bucket, key, tmp.name)
    return tmp.name


def clean_excel_dataframe(pdf):
    pdf.columns = [str(c).strip() for c in pdf.columns]

    long_columns = [
        "order_num",
        "order_id",
        "user_id",
        "id",
        "product_id",
        "department_id",
    ]

    int_columns = [
        "add_to_cart_order",
        "reordered",
    ]

    double_columns = [
        "total_amount",
        "days_since_prior_order",
    ]

    # NOTE: pd.to_numeric(errors="coerce") turns unparseable values into
    # NaN, not None. A plain Series.apply() that returns a mix of int/float
    # and None gets silently re-coerced back to NaN by pandas' dtype
    # inference, so we build a fresh object-dtype Series explicitly - that's
    # the only way to guarantee a real Python None survives into
    # spark.createDataFrame() as a SQL NULL rather than a numeric NaN (which
    # col(...).isNull() would fail to catch, and int(nan) would crash on).
    def _to_nullable(series, cast):
        return pd.Series(
            [cast(x) if pd.notnull(x) else None for x in series],
            index=series.index,
            dtype=object,
        )

    for c in long_columns:
        if c in pdf.columns:
            pdf[c] = _to_nullable(pd.to_numeric(pdf[c], errors="coerce"), int)

    for c in int_columns:
        if c in pdf.columns:
            pdf[c] = _to_nullable(pd.to_numeric(pdf[c], errors="coerce"), int)

    for c in double_columns:
        if c in pdf.columns:
            pdf[c] = _to_nullable(pd.to_numeric(pdf[c], errors="coerce"), float)

    for c in pdf.columns:
        if "timestamp" in c.lower() or c.lower() == "date":
            pdf[c] = pd.to_datetime(pdf[c], errors="coerce").astype(str)

    pdf = pdf.where(pd.notnull(pdf), None)
    return pdf


def read_excel_from_s3(key, schema):
    local_file = download_s3_object(key)
    pdf = pd.read_excel(local_file)
    pdf = clean_excel_dataframe(pdf)
    return spark.createDataFrame(pdf, schema=schema)


def write_rejected(df, table_name, reason):
    rejected = (
        df.withColumn("reject_reason", lit(reason))
        .withColumn("rejected_at", current_timestamp())
    )
    rejected.write.mode("append").json(s3_path(f"{rejected_prefix}/{table_name}/"))


def validate_required_columns(df, table_name, required):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{table_name} is missing required columns: {missing}")


def split_valid_invalid(df, id_columns, timestamp_column=None, business_rule_condition=None):
    invalid_condition = None

    for c in id_columns:
        cond = col(c).isNull()
        invalid_condition = cond if invalid_condition is None else (invalid_condition | cond)

    if timestamp_column:
        cond = to_timestamp(col(timestamp_column)).isNull()
        invalid_condition = cond if invalid_condition is None else (invalid_condition | cond)

    if business_rule_condition is not None:
        invalid_condition = (
            business_rule_condition if invalid_condition is None else (invalid_condition | business_rule_condition)
        )

    invalid = df.filter(invalid_condition) if invalid_condition is not None else df.limit(0)
    valid = df.subtract(invalid)

    return valid, invalid


def write_delta_upsert(df, table_name, keys, partition_cols=None):
    path = s3_path(f"{processed_prefix}/{table_name}")
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])

        (
            target.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(path)

    spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
    spark.sql(f"CREATE TABLE {database}.{table_name} USING DELTA LOCATION '{path}'")


def process_products():
    df = (
        spark.read.option("header", True)
        .schema(PRODUCT_SCHEMA)
        .csv(s3_path(f"{raw_prefix}/products/products.csv"))
    )
    read_count = df.count()
    logger.info("products: read %d row(s) from raw zone", read_count)

    required = ["product_id", "department_id", "department", "product_name"]
    validate_required_columns(df, "products", required)

    valid, invalid = split_valid_invalid(df, ["product_id"])
    invalid_count = invalid.count()

    if invalid_count > 0:
        logger.warning("products: rejecting %d row(s) with null product_id", invalid_count)
        write_rejected(invalid, "products", "Null product_id")

    valid = valid.dropDuplicates(["product_id"])
    written_count = valid.count()
    write_delta_upsert(valid, "products", ["product_id"], ["department"])
    logger.info(
        "products: read=%d rejected=%d written=%d",
        read_count, invalid_count, written_count,
    )


def process_orders():
    df = read_excel_from_s3(
        f"{raw_prefix}/orders/orders_apr_2025.xlsx",
        ORDERS_SCHEMA
    )
    read_count = df.count()
    logger.info("orders: read %d row(s) from raw zone", read_count)

    required = ["order_id", "user_id", "order_timestamp", "total_amount", "date"]
    validate_required_columns(df, "orders", required)

    df = (
        df.withColumn("order_ts", to_timestamp(col("order_timestamp")))
        .withColumn("order_date", to_date(col("date")))
    )

    # Business rule: total_amount is a currency value and must not be negative.
    negative_amount = col("total_amount").isNull() | (col("total_amount") < 0)

    valid, invalid = split_valid_invalid(
        df, ["order_id", "user_id"], "order_timestamp",
        business_rule_condition=negative_amount,
    )
    invalid_count = invalid.count()

    if invalid_count > 0:
        logger.warning(
            "orders: rejecting %d row(s) with invalid keys/timestamp or negative total_amount",
            invalid_count,
        )
        write_rejected(invalid, "orders", "Invalid order_id/user_id/order_timestamp or negative total_amount")

    valid = valid.dropDuplicates(["order_id"])
    written_count = valid.count()
    write_delta_upsert(valid, "orders", ["order_id"], ["order_date"])
    logger.info(
        "orders: read=%d rejected=%d written=%d",
        read_count, invalid_count, written_count,
    )


def process_order_items():
    df = read_excel_from_s3(
        f"{raw_prefix}/order_items/order_items_apr_2025.xlsx",
        ORDER_ITEMS_SCHEMA
    )
    read_count = df.count()
    logger.info("order_items: read %d row(s) from raw zone", read_count)

    required = ["id", "order_id", "user_id", "product_id", "order_timestamp", "date"]
    validate_required_columns(df, "order_items", required)

    df = (
        df.withColumn("order_ts", to_timestamp(col("order_timestamp")))
        .withColumn("order_date", to_date(col("date")))
    )

    # Business rule: days_since_prior_order may be null (first order) but
    # must not be negative when present.
    negative_days_since_prior_order = col("days_since_prior_order") < 0

    valid, invalid = split_valid_invalid(
        df,
        ["id", "order_id", "user_id", "product_id"],
        "order_timestamp",
        business_rule_condition=negative_days_since_prior_order,
    )

    products = (
        spark.read.format("delta")
        .load(s3_path(f"{processed_prefix}/products"))
        .select("product_id")
        .dropDuplicates()
    )

    orders = (
        spark.read.format("delta")
        .load(s3_path(f"{processed_prefix}/orders"))
        .select("order_id")
        .dropDuplicates()
    )

    with_product = valid.join(products, "product_id", "left_semi")
    missing_product = valid.join(products, "product_id", "left_anti")

    with_order = with_product.join(orders, "order_id", "left_semi")
    missing_order = with_product.join(orders, "order_id", "left_anti")

    rejected = (
        invalid.unionByName(missing_product, allowMissingColumns=True)
        .unionByName(missing_order, allowMissingColumns=True)
    )
    rejected_count = rejected.count()

    if rejected_count > 0:
        logger.warning(
            "order_items: rejecting %d row(s) for invalid keys or referential integrity failure",
            rejected_count,
        )
        write_rejected(
            rejected,
            "order_items",
            "Invalid keys or referential integrity failure"
        )

    clean = with_order.dropDuplicates(["id"])
    written_count = clean.count()
    write_delta_upsert(clean, "order_items", ["id"], ["order_date"])
    logger.info(
        "order_items: read=%d rejected=%d written=%d",
        read_count, rejected_count, written_count,
    )


def main():
    stages = [
        ("products", process_products),
        ("orders", process_orders),
        ("order_items", process_order_items),
    ]

    for stage_name, stage_fn in stages:
        try:
            stage_fn()
        except Exception:
            logger.exception("Lakehouse ETL failed while processing stage=%s", stage_name)
            raise

    logger.info("Lakehouse ETL completed successfully for all stages")


if __name__ == "__main__":
    bootstrap()
    try:
        main()
    finally:
        spark.stop()