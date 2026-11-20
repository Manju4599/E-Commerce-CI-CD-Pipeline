
import io
import hashlib
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from src.silver_versioning import verify_silver_artifact
from src.s3_ingestion import BUCKET_NAME


def make_manifest(content):
    return {
        "silver_version": 1,
        "file": "silver/orders/orders_v001.csv",
        "sha256": hashlib.sha256(content).hexdigest(),
        "last_processed_order_id": 112,
        "committed_batches": ["orders_109_109"],
    }


def test_verify_valid_silver_artifact():

    content = b"order_id,customer_id,amount\n109,1,500.00\n"
    manifest = make_manifest(content)

    with patch(
        "src.silver_versioning.s3.get_object",
        return_value={"Body": io.BytesIO(content)},
    ) as mock_get:

        result = verify_silver_artifact(manifest)

    assert result is True

    mock_get.assert_called_once_with(
        Bucket=BUCKET_NAME,
        Key=manifest["file"],
    )


def test_verify_missing_silver_artifact():

    manifest = make_manifest(b"some silver data")

    error = ClientError(
        {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "Object not found",
            }
        },
        "GetObject",
    )

    with patch(
        "src.silver_versioning.s3.get_object",
        side_effect=error,
    ):

        result = verify_silver_artifact(manifest)

    assert result is False


def test_verify_checksum_mismatch():

    expected_content = b"correct silver data"
    actual_content = b"corrupted silver data"

    manifest = make_manifest(expected_content)

    with patch(
        "src.silver_versioning.s3.get_object",
        return_value={"Body": io.BytesIO(actual_content)},
    ):

        result = verify_silver_artifact(manifest)

    assert result is False


def test_verify_propagates_unexpected_s3_error():

    manifest = make_manifest(b"some silver data")

    error = ClientError(
        {
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access denied",
            }
        },
        "GetObject",
    )

    with patch(
        "src.silver_versioning.s3.get_object",
        side_effect=error,
    ):

        with pytest.raises(ClientError):
            verify_silver_artifact(manifest)


def test_verify_rejects_invalid_checksum():

    manifest = {
        "silver_version": 1,
        "file": "silver/orders/orders_v001.csv",
        "sha256": "invalid-checksum",
    }

    with pytest.raises(
        ValueError,
        match="SHA-256 checksum is invalid",
    ):

        verify_silver_artifact(manifest)


def test_verify_rejects_missing_manifest_fields():

    manifest = {
        "silver_version": 1,
    }

    with pytest.raises(
        ValueError,
        match="Missing manifest fields",
    ):

        verify_silver_artifact(manifest)
