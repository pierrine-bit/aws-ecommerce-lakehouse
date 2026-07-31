import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Standard retry mode gives us automatic exponential backoff on throttling
# and transient 5xx errors from S3, on top of the per-object handling below.
s3 = boto3.client("s3", config=Config(retries={"mode": "standard", "max_attempts": 5}))


def archive_object(bucket, key, dest_key):
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": key},
        Key=dest_key,
    )
    s3.delete_object(Bucket=bucket, Key=key)


def lambda_handler(event, context):
    bucket = os.environ["BUCKET"]
    source_prefix = os.environ.get("SOURCE_PREFIX", "raw").strip("/")
    archive_prefix = os.environ.get("ARCHIVE_PREFIX", "archived").strip("/")

    if not bucket:
        raise ValueError("BUCKET environment variable is required")

    stamp = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")
    logger.info(
        "Starting archive run: bucket=%s source_prefix=%s archive_prefix=%s stamp=%s",
        bucket, source_prefix, archive_prefix, stamp,
    )

    paginator = s3.get_paginator("list_objects_v2")
    moved = []
    failed = []

    for page in paginator.paginate(Bucket=bucket, Prefix=f"{source_prefix}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue

            dest_key = f"{archive_prefix}/{stamp}/{key}"
            try:
                archive_object(bucket, key, dest_key)
                moved.append({"from": key, "to": dest_key})
            except ClientError:
                # Log and keep going so one bad object doesn't block the
                # rest of the raw zone from being archived.
                logger.exception("Failed to archive key=%s", key)
                failed.append(key)

    logger.info(
        "Archive run finished: archived_count=%d failed_count=%d",
        len(moved), len(failed),
    )

    if failed:
        raise RuntimeError(
            f"Archived {len(moved)} object(s), but failed to archive {len(failed)}: {failed[:20]}"
        )

    return {"archived_count": len(moved), "archived": moved[:20]}
