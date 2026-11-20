
import logging
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def upload_gold_to_s3(
    local_gold_path: str = "data/lakehouse/gold",
    bucket_name: str = "nyc-taxi-etl-gold-unique-id",
    s3_prefix: str = "nyc-taxi/gold/",
) -> None:

    gold_path = Path(local_gold_path)

    if not gold_path.exists():
        raise FileNotFoundError(
            f"Gold directory does not exist: {gold_path}"
        )

    parquet_files = list(gold_path.rglob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found in: {gold_path}"
        )

    s3_client = boto3.client("s3", region_name="us-east-1")

    for file_path in parquet_files:

        relative_path = file_path.relative_to(gold_path)

        s3_key = f"{s3_prefix}{relative_path.as_posix()}"

        try:
            logger.info(
                "Uploading %s to s3://%s/%s",
                file_path,
                bucket_name,
                s3_key,
            )

            s3_client.upload_file(
                str(file_path),
                bucket_name,
                s3_key,
            )

            logger.info("Upload successful: %s", s3_key)

        except ClientError:
            logger.exception("Failed to upload %s", file_path)
            raise

    logger.info(
        "Successfully uploaded %d Gold Parquet file(s) to S3.",
        len(parquet_files),
    )
