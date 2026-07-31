import logging
import sys
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("ecommerce-lakehouse-quality-checks")

REQUIRED_TABLES = ["products", "orders", "order_items"]


def check_table(s3, glue, bucket, processed_prefix, database, table):
    """Returns (delta_log_present, catalog_entry_present, latest_commit_at).

    latest_commit_at is the newest LastModified timestamp under the table's
    _delta_log/ prefix, used by evaluate_tables() to flag stale data - a
    Delta log left over from a previous successful run would otherwise pass
    a presence-only check even if today's run silently wrote zero rows.
    """
    latest_commit_at = None

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{processed_prefix}/{table}/_delta_log/"):
            for obj in page.get("Contents", []):
                if latest_commit_at is None or obj["LastModified"] > latest_commit_at:
                    latest_commit_at = obj["LastModified"]
    except ClientError:
        logger.exception("Failed to check Delta log for table=%s", table)
        raise

    catalog_entry_present = True
    try:
        glue.get_table(DatabaseName=database, Name=table)
    except glue.exceptions.EntityNotFoundException:
        catalog_entry_present = False
    except ClientError:
        logger.exception("Failed to check Glue Catalog entry for table=%s", table)
        raise

    return latest_commit_at is not None, catalog_entry_present, latest_commit_at


def evaluate_tables(s3, glue, bucket, processed_prefix, database, max_data_age_hours, tables=REQUIRED_TABLES, now=None):
    """Runs the presence/catalog/freshness checks for `tables`.

    Kept separate from bootstrap()/main() so it can be unit tested with
    mocked S3/Glue clients instead of needing a live Glue job context.
    """
    now = now or datetime.now(timezone.utc)
    missing_delta_log = []
    missing_catalog_entry = []
    stale_tables = []

    for table in tables:
        delta_log_present, catalog_entry_present, latest_commit_at = check_table(
            s3, glue, bucket, processed_prefix, database, table
        )

        age_hours = None
        if latest_commit_at is not None:
            age_hours = (now - latest_commit_at).total_seconds() / 3600

        logger.info(
            "table=%s delta_log_present=%s catalog_entry_present=%s last_commit_age_hours=%s",
            table, delta_log_present, catalog_entry_present,
            f"{age_hours:.2f}" if age_hours is not None else "unknown",
        )

        if not delta_log_present:
            missing_delta_log.append(table)
        if not catalog_entry_present:
            missing_catalog_entry.append(table)
        if age_hours is not None and age_hours > max_data_age_hours:
            stale_tables.append(table)

    return missing_delta_log, missing_catalog_entry, stale_tables


def run_quality_gate(s3, glue, bucket, processed_prefix, database, max_data_age_hours, now=None):
    missing_delta_log, missing_catalog_entry, stale_tables = evaluate_tables(
        s3, glue, bucket, processed_prefix, database, max_data_age_hours, now=now
    )

    if missing_delta_log:
        raise RuntimeError(f"Missing Delta transaction logs for tables: {missing_delta_log}")

    if missing_catalog_entry:
        raise RuntimeError(f"Missing Glue Data Catalog entries for tables: {missing_catalog_entry}")

    if stale_tables:
        raise RuntimeError(
            f"Delta tables have not been updated within {max_data_age_hours}h: {stale_tables}"
        )

    logger.info("Quality checks passed for tables: %s", REQUIRED_TABLES)
    return REQUIRED_TABLES


def bootstrap():
    """Resolves Glue job arguments and builds the AWS clients.

    Kept separate from module import (and using a local awsglue import) so
    tests can exercise evaluate_tables()/run_quality_gate() without the
    awsglue package, which is only available inside an actual Glue job.
    """
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ["bucket", "processed_prefix", "database", "max_data_age_hours"])

    # Standard retry mode adds exponential backoff for throttling and
    # transient 5xx errors so a brief AWS API blip doesn't fail the gate.
    retry_config = Config(retries={"mode": "standard", "max_attempts": 5})

    return {
        "s3": boto3.client("s3", config=retry_config),
        "glue": boto3.client("glue", config=retry_config),
        "bucket": args["bucket"],
        "processed_prefix": args["processed_prefix"].strip("/"),
        "database": args["database"],
        "max_data_age_hours": float(args["max_data_age_hours"]),
    }


if __name__ == "__main__":
    ctx = bootstrap()
    run_quality_gate(
        ctx["s3"], ctx["glue"], ctx["bucket"], ctx["processed_prefix"], ctx["database"], ctx["max_data_age_hours"]
    )
