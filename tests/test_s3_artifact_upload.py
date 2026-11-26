
from unittest.mock import patch

import pytest

from src.s3_ingestion import upload_artifact


def test_upload_artifact_success(tmp_path):
    artifact = tmp_path / "orders.parquet"
    artifact.write_bytes(b"sample parquet data")

    checksum = "a" * 64

    with (
        patch(
            "src.s3_ingestion.calculate_sha256",
            return_value=checksum,
        ),
        patch(
            "src.s3_ingestion.s3.upload_file",
        ) as mock_upload,
        patch(
            "src.s3_ingestion.s3.head_object",
            return_value={
                "Metadata": {"sha256": checksum}
            },
        ),
    ):
        result = upload_artifact(
            artifact,
            "silver/orders/orders_v001.parquet",
            "application/vnd.apache.parquet",
        )

    assert result == checksum

    mock_upload.assert_called_once_with(
        str(artifact),
        "de-learning-lake",
        "silver/orders/orders_v001.parquet",
        ExtraArgs={
            "ContentType": "application/vnd.apache.parquet",
            "Metadata": {"sha256": checksum},
        },
    )


def test_upload_artifact_checksum_mismatch(tmp_path):
    artifact = tmp_path / "orders.parquet"
    artifact.write_bytes(b"sample parquet data")

    with (
        patch(
            "src.s3_ingestion.calculate_sha256",
            return_value="a" * 64,
        ),
        patch("src.s3_ingestion.s3.upload_file"),
        patch(
            "src.s3_ingestion.s3.head_object",
            return_value={
                "Metadata": {"sha256": "b" * 64}
            },
        ),
    ):
        with pytest.raises(
            RuntimeError,
            match="checksum verification failed",
        ):
            upload_artifact(
                artifact,
                "silver/orders/orders_v001.parquet",
                "application/vnd.apache.parquet",
            )


def test_upload_artifact_missing_file(tmp_path):
    artifact = tmp_path / "missing.parquet"

    with patch(
        "src.s3_ingestion.s3.upload_file"
    ) as mock_upload:
        with pytest.raises(FileNotFoundError):
            upload_artifact(
                artifact,
                "silver/orders/missing.parquet",
                "application/vnd.apache.parquet",
            )

    mock_upload.assert_not_called()
