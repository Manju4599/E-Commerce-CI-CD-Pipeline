
import hashlib
import hmac
import logging
import tempfile
from pathlib import Path

import pandas as pd
from botocore.exceptions import ClientError

from src.s3_ingestion import (
    s3,
    BUCKET_NAME,
    calculate_sha256,
    upload_csv,
    upload_artifact,
)
from src.parquet_storage import write_parquet_atomically
from src.silver_manifest import (
    read_silver_manifest,
    write_silver_manifest,
)


logger = logging.getLogger(__name__)

PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"


def _is_missing_object_error(error):
    error_code = error.response.get(
        "Error", {}
    ).get("Code", "")

    return error_code in ("404", "NoSuchKey", "NotFound")


def _verify_s3_object(s3_key, expected_hash):
    """Verify an S3 object's actual bytes against its SHA-256 checksum."""

    try:
        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
        )
    except ClientError as error:
        if _is_missing_object_error(error):
            return False
        raise

    body = response["Body"]
    sha256 = hashlib.sha256()

    try:
        while True:
            chunk = body.read(8192)

            if not chunk:
                break

            sha256.update(chunk)
    finally:
        body.close()

    actual_hash = sha256.hexdigest()

    return hmac.compare_digest(
        actual_hash.lower(),
        expected_hash.lower(),
    )


def verify_silver_artifact(manifest):
    """
    Verify the CSV artifact and, when present, its Parquet counterpart.

    Legacy CSV-only manifests remain supported.
    """

    required_fields = {"file", "sha256"}
    missing_fields = required_fields - manifest.keys()

    if missing_fields:
        raise ValueError(
            f"Missing manifest fields: {missing_fields}"
        )

    csv_key = manifest["file"]
    csv_hash = manifest["sha256"]

    if not isinstance(csv_key, str) or not csv_key:
        raise ValueError(
            "Manifest file key must be a non-empty string"
        )

    def valid_sha256(value):
        return (
            isinstance(value, str)
            and len(value) == 64
            and all(
                char in "0123456789abcdefABCDEF"
                for char in value
            )
        )

    if not valid_sha256(csv_hash):
        raise ValueError(
            "Manifest SHA-256 checksum is invalid"
        )

    # First verify the original CSV artifact.
    
    # Validate optional Parquet metadata before accessing S3.
    has_parquet_file = "parquet_file" in manifest
    has_parquet_hash = "parquet_sha256" in manifest

    if has_parquet_file != has_parquet_hash:
        raise ValueError(
            "Manifest must contain both parquet_file "
            "and parquet_sha256"
        )

    # Legacy CSV-only manifest.
    if not has_parquet_file:
        if not _verify_s3_object(csv_key, csv_hash):
            return False
        return True

    parquet_key = manifest["parquet_file"]
    parquet_hash = manifest["parquet_sha256"]

    if not isinstance(parquet_key, str) or not parquet_key:
        raise ValueError(
            "Manifest Parquet file key must be a non-empty string"
        )

    if not valid_sha256(parquet_hash):
        raise ValueError(
            "Manifest Parquet SHA-256 checksum is invalid"
        )

    # Both manifest formats are valid. Now verify actual S3 bytes.
    if not _verify_s3_object(csv_key, csv_hash):
        return False

    return _verify_s3_object(
        parquet_key,
        parquet_hash,
    )



def _ensure_immutable_artifact(
    file_path,
    s3_key,
    local_hash,
    upload_function,
):
    """
    Reuse an identical existing object or upload a missing object.

    Never overwrite an existing version with different content.
    """

    try:
        existing = s3.head_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
        )
    except ClientError as error:
        if not _is_missing_object_error(error):
            raise
        existing = None

    if existing is not None:
        existing_hash = existing.get(
            "Metadata", {}
        ).get("sha256")

        if existing_hash != local_hash:
            raise RuntimeError(
                "Immutable Silver version already exists "
                f"with different content: {s3_key}"
            )

        logger.info(
            "Reusing existing Silver artifact: %s",
            s3_key,
        )
        return

    upload_function(file_path, s3_key)


def publish_versioned_silver(
    file_path,
    processed_batches,
    manifest_key="silver/orders/manifest.json",
    silver_prefix="silver/orders",
):
    """
    Publish an immutable CSV and Parquet Silver version.

    The manifest is updated only after both uploaded artifacts
    have passed byte-level checksum verification.
    """

    file_path = Path(file_path)

    if not file_path.is_file():
        raise FileNotFoundError(
            f"Silver file not found: {file_path}"
        )

    if not processed_batches:
        raise ValueError("No processed batches supplied.")

    csv_hash = calculate_sha256(file_path)

    if manifest_key == "silver/orders/manifest.json":
        current_manifest = read_silver_manifest()
    else:
        current_manifest = read_silver_manifest(manifest_key)

    if current_manifest is None:
        version = 1
        committed_batches = []
        last_processed_id = 0
    else:
        version = current_manifest["silver_version"] + 1
        committed_batches = list(
            current_manifest["committed_batches"]
        )
        last_processed_id = current_manifest[
            "last_processed_order_id"
        ]

    version_prefix = (
        f"{silver_prefix.rstrip('/')}/"
        f"orders_v{version:03d}"
    )

    csv_key = f"{version_prefix}.csv"
    parquet_key = f"{version_prefix}.parquet"

    # Generate Parquet from the canonical consolidated CSV.
    # TemporaryDirectory cleans up the local Parquet file on success
    # and on every failure path.
    with tempfile.TemporaryDirectory(
        prefix="silver_parquet_"
    ) as temporary_directory:

        parquet_path = (
            Path(temporary_directory) / "orders.parquet"
        )

        silver_df = pd.read_csv(file_path)

        write_parquet_atomically(
            silver_df,
            parquet_path,
        )

        parquet_hash = calculate_sha256(parquet_path)

        # Publish or reuse CSV.
        _ensure_immutable_artifact(
            file_path=file_path,
            s3_key=csv_key,
            local_hash=csv_hash,
            upload_function=upload_csv,
        )

        # Publish or reuse Parquet.
        _ensure_immutable_artifact(
            file_path=parquet_path,
            s3_key=parquet_key,
            local_hash=parquet_hash,
            upload_function=lambda path, key: upload_artifact(
                path,
                key,
                PARQUET_CONTENT_TYPE,
            ),
        )

        for batch in processed_batches:
            batch_id = batch["batch_id"]

            if batch_id not in committed_batches:
                committed_batches.append(batch_id)

            last_processed_id = max(
                last_processed_id,
                batch["last_id"],
            )

        new_manifest = {
            "silver_version": version,
            "file": csv_key,
            "sha256": csv_hash,
            "parquet_file": parquet_key,
            "parquet_sha256": parquet_hash,
            "last_processed_order_id": last_processed_id,
            "committed_batches": committed_batches,
        }

        # This verifies the actual bytes of both S3 objects.
        if not verify_silver_artifact(new_manifest):
            raise RuntimeError(
                "Silver artifact verification failed "
                f"for version {version}"
            )

        # Manifest update is the publication commit point.
        if manifest_key == "silver/orders/manifest.json":
            write_silver_manifest(new_manifest)
        else:
            write_silver_manifest(
                new_manifest,
                manifest_key,
            )

    logger.info(
        "Silver version %d committed successfully "
        "with CSV and Parquet artifacts.",
        version,
    )

    return new_manifest
