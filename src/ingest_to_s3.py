
from pathlib import Path
import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError


# 1. Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# 2. Define source and destination
PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_FILE = PROJECT_ROOT / "verify_order.csv"
BUCKET_NAME = "de-learning-lake"
S3_KEY = "bronze/orders"


# 3. Create the S3 client
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost.floci.io:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)


def ingest_csv():
    try:
        # Validate the input file
        if not SOURCE_FILE.is_file():
            raise FileNotFoundError(
                f"Source file not found: {SOURCE_FILE}"
            )

        logger.info("Starting S3 ingestion")
        logger.info("Source file: %s", SOURCE_FILE)

        # Upload the CSV
        s3.upload_file(
            str(SOURCE_FILE),
            BUCKET_NAME,
            S3_KEY,
            ExtraArgs={"ContentType": "text/csv"},
        )

        # Verify the uploaded object's metadata
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
    ingest_csv()
