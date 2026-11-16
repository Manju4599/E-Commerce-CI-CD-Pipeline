
import logging
import os
from pathlib import Path
from src.file_integrity import calculate_sha256
import boto3
from botocore.exceptions import BotoCoreError, ClientError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

BUCKET_NAME = "de-learning-lake"

# Use local Floci settings by default.
# These can be overridden through environment variables.
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv(
        "AWS_ENDPOINT_URL",
        "http://localhost.floci.io:4566",
    ),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
)



def upload_csv(file_path: Path, s3_key: str):
    """Upload a CSV only when its content has changed."""

    if not file_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    local_hash = calculate_sha256(file_path)

    # Check whether the object already exists in S3.
    try:
        existing = s3.head_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
        )
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "")

        # A missing object is expected on its first upload.
        if error_code not in ("404", "NoSuchKey", "NotFound"):
            raise

        existing = None

    # Skip uploading if the saved hash matches the local file.
    if (
        existing is not None
        and existing.get("Metadata", {}).get("sha256") == local_hash
    ):
        logging.info(
            "Skipping unchanged file: s3://%s/%s",
            BUCKET_NAME,
            s3_key,
        )
        return

    # Upload changed or previously unseen content.
    s3.upload_file(
        str(file_path),
        BUCKET_NAME,
        s3_key,
        ExtraArgs={
            "ContentType": "text/csv",
            "Metadata": {"sha256": local_hash},
        },
    )

    # Verify the uploaded object.
    result = s3.head_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
    )

    saved_hash = result.get("Metadata", {}).get("sha256")

    if saved_hash != local_hash:
        raise RuntimeError(
            f"SHA-256 metadata verification failed for {s3_key}"
        )

    logging.info(
        "Uploaded %s to s3://%s/%s (%s bytes)",
        file_path.name,
        BUCKET_NAME,
        s3_key,
        result["ContentLength"],
    )

def download_csv(s3_key: str, destination_path: Path):
    """Download a CSV from S3 and verify its SHA-256 checksum."""

    import tempfile

    # Retrieve expected checksum from S3 metadata.
    try:
        response = s3.head_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
        )

    except ClientError as error:
        raise RuntimeError(
            f"Unable to find S3 object: {s3_key}"
        ) from error

    expected_hash = response.get(
        "Metadata", {}
    ).get("sha256")

    if not expected_hash:
        raise RuntimeError(
            f"SHA-256 metadata missing for {s3_key}"
        )

    # Reuse an existing local file only if its checksum matches.
    if destination_path.is_file():

        local_hash = calculate_sha256(destination_path)

        if local_hash == expected_hash:
            logging.info(
                "Using verified local Bronze file: %s",
                destination_path,
            )
            return destination_path

        logging.warning(
            "Local Bronze checksum mismatch: %s",
            destination_path,
        )
    destination_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(
            dir=destination_path.parent,
            prefix=".bronze_",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)

        s3.download_file(
            BUCKET_NAME,
            s3_key,
            str(temp_path),
        )

        downloaded_hash = calculate_sha256(temp_path)

        if downloaded_hash != expected_hash:
            raise RuntimeError(
                f"SHA-256 verification failed for {s3_key}"
            )

        # Publish only after successful verification.
        temp_path.replace(destination_path)

        logging.info(
            "Downloaded and verified Bronze file: %s",
            destination_path,
        )

        return destination_path

    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()

def upload_bronze_files():
    """Upload every extracted CSV file to the Bronze layer."""

    files = sorted(RAW_DIR.glob("*.csv"))

    if not files:
        raise FileNotFoundError(
            f"No extracted CSV files found in {RAW_DIR}"
        )

    for file_path in files:
        s3_key = f"bronze/{file_path.stem}/{file_path.name}"
        upload_csv(file_path, s3_key)


def upload_silver_orders():
    """Upload the transformed orders CSV to the Silver layer."""

    clean_orders = PROCESSED_DIR / "orders_clean.csv"

    upload_csv(
        clean_orders,
        "silver/orders/orders_clean.csv",
    )
