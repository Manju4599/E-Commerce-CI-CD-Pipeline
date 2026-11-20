
from pathlib import Path
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from src.silver_versioning import publish_versioned_silver


def make_batch(batch_id="orders_109_110", last_id=110):
    return {
        "batch_id": batch_id,
        "first_id": last_id - 1,
        "last_id": last_id,
    }


def make_missing_object_error():
    return ClientError(
        {
            "Error": {
                "Code": "404",
                "Message": "Not Found",
            }
        },
        "HeadObject",
    )


def test_first_silver_publication_creates_v001(tmp_path):

    silver_file = tmp_path / "orders_clean.csv"
    silver_file.write_text("test data", encoding="utf-8")

    checksum = "a" * 64

    with (
        patch(
            "src.silver_versioning.calculate_sha256",
            return_value=checksum,
        ),
        patch(
            "src.silver_versioning.read_silver_manifest",
            return_value=None,
        ),
        patch(
            "src.silver_versioning.s3.head_object",
            side_effect=make_missing_object_error(),
        ),
        patch(
            "src.silver_versioning.upload_csv",
        ) as mock_upload,
        patch(
            "src.silver_versioning.verify_silver_artifact",
            return_value=True,
        ) as mock_verify,
        patch(
            "src.silver_versioning.write_silver_manifest",
        ) as mock_write,
    ):

        result = publish_versioned_silver(
            silver_file,
            [make_batch()],
        )

    assert result["silver_version"] == 1
    assert result["file"] == "silver/orders/orders_v001.csv"
    assert result["sha256"] == checksum
    assert result["last_processed_order_id"] == 110
    assert result["committed_batches"] == ["orders_109_110"]

    mock_upload.assert_called_once_with(
        silver_file,
        "silver/orders/orders_v001.csv",
    )

    mock_verify.assert_called_once_with(result)
    mock_write.assert_called_once_with(result)


def test_second_publication_creates_v002(tmp_path):

    silver_file = tmp_path / "orders_clean.csv"
    silver_file.write_text("updated data", encoding="utf-8")

    current_manifest = {
        "silver_version": 1,
        "file": "silver/orders/orders_v001.csv",
        "sha256": "a" * 64,
        "last_processed_order_id": 108,
        "committed_batches": ["orders_107_108"],
    }

    with (
        patch(
            "src.silver_versioning.calculate_sha256",
            return_value="b" * 64,
        ),
        patch(
            "src.silver_versioning.read_silver_manifest",
            return_value=current_manifest,
        ),
        patch(
            "src.silver_versioning.s3.head_object",
            side_effect=make_missing_object_error(),
        ),
        patch(
            "src.silver_versioning.upload_csv",
        ) as mock_upload,
        patch(
            "src.silver_versioning.verify_silver_artifact",
            return_value=True,
        ),
        patch(
            "src.silver_versioning.write_silver_manifest",
        ) as mock_write,
    ):

        result = publish_versioned_silver(
            silver_file,
            [make_batch()],
        )

    assert result["silver_version"] == 2
    assert result["file"] == "silver/orders/orders_v002.csv"
    assert result["last_processed_order_id"] == 110

    assert result["committed_batches"] == [
        "orders_107_108",
        "orders_109_110",
    ]

    mock_upload.assert_called_once_with(
        silver_file,
        "silver/orders/orders_v002.csv",
    )

    mock_write.assert_called_once_with(result)


def test_existing_identical_orphaned_version_is_reused(tmp_path):

    silver_file = tmp_path / "orders_clean.csv"
    silver_file.write_text("test data", encoding="utf-8")

    checksum = "a" * 64

    current_manifest = {
        "silver_version": 1,
        "file": "silver/orders/orders_v001.csv",
        "sha256": "b" * 64,
        "last_processed_order_id": 108,
        "committed_batches": ["orders_107_108"],
    }

    with (
        patch(
            "src.silver_versioning.calculate_sha256",
            return_value=checksum,
        ),
        patch(
            "src.silver_versioning.read_silver_manifest",
            return_value=current_manifest,
        ),
        patch(
            "src.silver_versioning.s3.head_object",
            return_value={
                "Metadata": {"sha256": checksum}
            },
        ),
        patch(
            "src.silver_versioning.upload_csv",
        ) as mock_upload,
        patch(
            "src.silver_versioning.verify_silver_artifact",
            return_value=True,
        ) as mock_verify,
        patch(
            "src.silver_versioning.write_silver_manifest",
        ) as mock_write,
    ):

        result = publish_versioned_silver(
            silver_file,
            [make_batch()],
        )

    assert result["silver_version"] == 2

    mock_upload.assert_not_called()
    mock_verify.assert_called_once_with(result)
    mock_write.assert_called_once_with(result)


def test_existing_version_with_different_content_is_rejected(tmp_path):

    silver_file = tmp_path / "orders_clean.csv"
    silver_file.write_text("new data", encoding="utf-8")

    current_manifest = {
        "silver_version": 1,
        "file": "silver/orders/orders_v001.csv",
        "sha256": "b" * 64,
        "last_processed_order_id": 108,
        "committed_batches": ["orders_107_108"],
    }

    with (
        patch(
            "src.silver_versioning.calculate_sha256",
            return_value="a" * 64,
        ),
        patch(
            "src.silver_versioning.read_silver_manifest",
            return_value=current_manifest,
        ),
        patch(
            "src.silver_versioning.s3.head_object",
            return_value={
                "Metadata": {"sha256": "c" * 64}
            },
        ),
        patch(
            "src.silver_versioning.upload_csv",
        ) as mock_upload,
        patch(
            "src.silver_versioning.write_silver_manifest",
        ) as mock_write,
    ):

        with pytest.raises(
            RuntimeError,
            match="Immutable Silver version already exists",
        ):
            publish_versioned_silver(
                silver_file,
                [make_batch()],
            )

    mock_upload.assert_not_called()
    mock_write.assert_not_called()


def test_checksum_verification_failure_prevents_manifest_update(tmp_path):

    silver_file = tmp_path / "orders_clean.csv"
    silver_file.write_text("test data", encoding="utf-8")

    with (
        patch(
            "src.silver_versioning.calculate_sha256",
            return_value="a" * 64,
        ),
        patch(
            "src.silver_versioning.read_silver_manifest",
            return_value=None,
        ),
        patch(
            "src.silver_versioning.s3.head_object",
            side_effect=make_missing_object_error(),
        ),
        patch(
            "src.silver_versioning.upload_csv",
        ) as mock_upload,
        patch(
            "src.silver_versioning.verify_silver_artifact",
            return_value=False,
        ),
        patch(
            "src.silver_versioning.write_silver_manifest",
        ) as mock_write,
    ):

        with pytest.raises(
            RuntimeError,
            match="Silver artifact verification failed",
        ):
            publish_versioned_silver(
                silver_file,
                [make_batch()],
            )

    mock_upload.assert_called_once()
    mock_write.assert_not_called()


def test_manifest_write_failure_is_propagated(tmp_path):

    silver_file = tmp_path / "orders_clean.csv"
    silver_file.write_text("test data", encoding="utf-8")

    with (
        patch(
            "src.silver_versioning.calculate_sha256",
            return_value="a" * 64,
        ),
        patch(
            "src.silver_versioning.read_silver_manifest",
            return_value=None,
        ),
        patch(
            "src.silver_versioning.s3.head_object",
            side_effect=make_missing_object_error(),
        ),
        patch(
            "src.silver_versioning.upload_csv",
        ) as mock_upload,
        patch(
            "src.silver_versioning.verify_silver_artifact",
            return_value=True,
        ),
        patch(
            "src.silver_versioning.write_silver_manifest",
            side_effect=RuntimeError("Manifest write failed"),
        ),
    ):

        with pytest.raises(
            RuntimeError,
            match="Manifest write failed",
        ):
            publish_versioned_silver(
                silver_file,
                [make_batch()],
            )

    mock_upload.assert_called_once()
