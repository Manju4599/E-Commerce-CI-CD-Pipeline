
import hashlib
import hmac
from botocore.exceptions import ClientError
from pathlib import Path
from src.s3_ingestion import (
    s3,
    BUCKET_NAME,
    calculate_sha256,
    upload_csv,
)
from src.silver_manifest import (
    read_silver_manifest,
    write_silver_manifest,
)

def verify_silver_artifact(manifest):
    """
    Verify that the Silver file referenced by a manifest
    exists in S3 and matches its recorded SHA-256 checksum.

    Returns:
        True  - file exists and checksum matches.
        False - file is missing or checksum does not match.

    Unexpected S3 errors are propagated.
    """

    required_fields = {"file", "sha256"}

    missing_fields = required_fields - manifest.keys()

    if missing_fields:
        raise ValueError(
            f"Missing manifest fields: {missing_fields}"
        )

    s3_key = manifest["file"]
    expected_hash = manifest["sha256"]

    if not isinstance(s3_key, str) or not s3_key:
        raise ValueError("Manifest file key must be a non-empty string")

    if (
        not isinstance(expected_hash, str)
        or len(expected_hash) != 64
        or any(char not in "0123456789abcdefABCDEF"
               for char in expected_hash)
    ):
        raise ValueError("Manifest SHA-256 checksum is invalid")

    try:
        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
        )

    except ClientError as error:

        error_code = error.response.get(
            "Error", {}
        ).get("Code")

        if error_code in ("NoSuchKey", "404", "NotFound"):
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


def publish_versioned_silver(file_path, processed_batches):
    """
    Publish a new immutable Silver version.

    Update the manifest only after the uploaded artifact
    has been verified.
    """

    if not file_path.is_file():
        raise FileNotFoundError(
            f"Silver file not found: {file_path}"
        )

    if not processed_batches:
        raise ValueError(
            "No processed batches supplied."
        )

    # Step 1: Calculate the local artifact checksum.
    local_hash = calculate_sha256(file_path)

    # Step 2: Read the current committed manifest.
    current_manifest = read_silver_manifest()

    if current_manifest is None:
        version = 1
        committed_batches = []
        last_processed_id = 0
    else:
        version = current_manifest["silver_version"] + 1

        committed_batches = current_manifest["committed_batches"]

        last_processed_id = current_manifest[
            "last_processed_order_id"
        ]

    # Step 3: Generate a new immutable S3 key.
    s3_key = (
        f"silver/orders/orders_v{version:03d}.csv"
    )

    # Step 4: Do not overwrite an existing version.
    try:
        existing = s3.head_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
        )

    except ClientError as error:
        error_code = error.response.get(
            "Error", {}
        ).get("Code", "")

        if error_code not in ("404", "NoSuchKey", "NotFound"):
            raise

        existing = None

    if existing is not None:
        existing_hash = existing.get(
            "Metadata", {}
        ).get("sha256")

        if existing_hash != local_hash:
            raise RuntimeError(
                f"Immutable Silver version already exists "
                f"with different content: {s3_key}"
            )

    else:
        upload_csv(file_path, s3_key)

    # Step 5: Build the candidate manifest.
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
        "file": s3_key,
        "sha256": local_hash,
        "last_processed_order_id": last_processed_id,
        "committed_batches": committed_batches,
    }

    # Step 6: Verify actual S3 object bytes.
    if not verify_silver_artifact(new_manifest):
        raise RuntimeError(
            f"Silver artifact verification failed: {s3_key}"
        )

    # Step 7: Publish the manifest as the commit record.
    write_silver_manifest(new_manifest)

    print(
        f"Silver version {version} committed successfully."
    )

    return new_manifest
