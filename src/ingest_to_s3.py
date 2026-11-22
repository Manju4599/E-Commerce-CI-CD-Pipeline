
from pathlib import Path
import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from config.settings import (
    PROJECT_ROOT,
    S3_BUCKET_NAME,
    AWS_ENDPOINT_URL,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_DEFAULT_REGION,
)

logger = logging.getLogger(__name__)

SOURCE_FILE = PROJECT_ROOT / "verify_order.csv"
BUCKET_NAME = S3_BUCKET_NAME
S3_KEY = "bronze/orders"

s3 = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION,
)


def ingest_csv():
    try:
        if not SOURCE_FILE.is_file():
            raise FileNotFoundError(
                f"Source file not found: {SOURCE_FILE}"
            )

        logger.info("Starting S3 ingestion")
        logger.info("Source file: %s", SOURCE_FILE)

        s3.upload_file(
            str(SOURCE_FILE),
            BUCKET_NAME,
            S3_KEY,
            ExtraArgs={"ContentType": "text/csv"},
        )

        result = s3.head_object(
            Bucket=BUCKET_NAME,
            Key=S3_KEY,
        )

        logger.info("Upload completed successfully")
        logger.info("S3 location: s3://%s/%s", BUCKET_NAME, S3_KEY)
        logger.info("File size: %s bytes", result["ContentLength"])
        logger.info("Content type: %s", result["ContentType"])

    except FileNotFoundError:
        logger.exception("Ingestion stopped: source file is missing")
        raise

    except (ClientError, BotoCoreError):
        logger.exception("Ingestion failed because of an S3 error")
        raise


if __name__ == "__main__":
    from src.logging_config import configure_logging

    configure_logging()
    ingest_csv()