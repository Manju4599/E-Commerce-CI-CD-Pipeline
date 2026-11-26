
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


def test_verify_valid_csv_and_parquet_artifacts():
    csv_content = b"order_id,amount\n109,500\n"
    parquet_content = b"sample parquet bytes"

    manifest = make_manifest(csv_content)
    manifest["parquet_file"] = (
        "silver/orders/orders_v001.parquet"
    )
    manifest["parquet_sha256"] = hashlib.sha256(
        parquet_content
    ).hexdigest()

    with patch(
        "src.silver_versioning.s3.get_object",
        side_effect=[
            {"Body": io.BytesIO(csv_content)},
            {"Body": io.BytesIO(parquet_content)},
        ],
    ) as mock_get:

        result = verify_silver_artifact(manifest)

    assert result is True
    assert mock_get.call_count == 2


def test_verify_missing_parquet_artifact():
    csv_content = b"order_id,amount\n109,500\n"
    parquet_content = b"sample parquet bytes"

    manifest = make_manifest(csv_content)
    manifest["parquet_file"] = (
        "silver/orders/orders_v001.parquet"
    )
    manifest["parquet_sha256"] = hashlib.sha256(
        parquet_content
    ).hexdigest()

    missing_error = ClientError(
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
        side_effect=[
            {"Body": io.BytesIO(csv_content)},
            missing_error,
        ],
    ):
        result = verify_silver_artifact(manifest)

    assert result is False


def test_verify_corrupted_parquet_artifact():
    csv_content = b"order_id,amount\n109,500\n"
    expected_parquet = b"correct parquet bytes"
    corrupted_parquet = b"corrupted parquet bytes"

    manifest = make_manifest(csv_content)
    manifest["parquet_file"] = (
        "silver/orders/orders_v001.parquet"
    )
    manifest["parquet_sha256"] = hashlib.sha256(
        expected_parquet
    ).hexdigest()

    with patch(
        "src.silver_versioning.s3.get_object",
        side_effect=[
            {"Body": io.BytesIO(csv_content)},
            {"Body": io.BytesIO(corrupted_parquet)},
        ],
    ):
        result = verify_silver_artifact(manifest)

    assert result is False


def test_verify_legacy_csv_only_manifest():
    content = b"legacy silver data"
    manifest = make_manifest(content)

    with patch(
        "src.silver_versioning.s3.get_object",
        return_value={"Body": io.BytesIO(content)},
    ) as mock_get:

        result = verify_silver_artifact(manifest)

    assert result is True
    mock_get.assert_called_once()


def test_verify_rejects_incomplete_parquet_metadata():
    content = b"silver data"
    manifest = make_manifest(content)

    manifest["parquet_file"] = (
        "silver/orders/orders_v001.parquet"
    )

    with pytest.raises(
        ValueError,
        match="both parquet_file and parquet_sha256",
    ):
        verify_silver_artifact(manifest)


def test_verify_corrupted_csv_with_parquet_manifest():
    expected_csv = b"correct csv"
    corrupted_csv = b"corrupted csv"

    manifest = make_manifest(expected_csv)
    manifest["parquet_file"] = (
        "silver/orders/orders_v001.parquet"
    )
    manifest["parquet_sha256"] = "a" * 64

    with patch(
        "src.silver_versioning.s3.get_object",
        return_value={"Body": io.BytesIO(corrupted_csv)},
    ):
        result = verify_silver_artifact(manifest)

    assert result is False
