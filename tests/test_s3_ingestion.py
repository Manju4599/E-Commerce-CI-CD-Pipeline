from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError
import pytest

from src.s3_ingestion import upload_csv, upload_bronze_files, BUCKET_NAME
from src.file_integrity import calculate_sha256
from pathlib import Path

def test_upload_missing_file_raises_error(tmp_path):
    missing_file = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):
        upload_csv(missing_file, "bronze/orders/missing.csv")


def test_bronze_fails_when_no_csv_files_exist(tmp_path):
    with patch("src.s3_ingestion.RAW_DIR", tmp_path):
        with pytest.raises(FileNotFoundError):
            upload_bronze_files()



def test_upload_calls_s3_and_verifies_metadata(tmp_path):
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text("order_id,amount\n101,500\n", encoding="utf-8")

    fake_metadata = {
        "ContentLength": csv_file.stat().st_size,
        "ContentType": "text/csv",
    }

    with (
        patch("src.s3_ingestion.s3.upload_file") as mock_upload,
        patch(
            "src.s3_ingestion.s3.head_object",
            return_value=fake_metadata,
        ) as mock_head,
    ):
        upload_csv(csv_file, "bronze/orders/orders.csv")

    mock_upload.assert_called_once_with(
        str(csv_file),
        "de-learning-lake",
        "bronze/orders/orders.csv",
        ExtraArgs={"ContentType": "text/csv"},
    )

    mock_head.assert_called_once_with(
        Bucket="de-learning-lake",
        Key="bronze/orders/orders.csv",
    )

def test_s3_upload_error_is_propagated(tmp_path):
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text("order_id,amount\n101,500\n", encoding="utf-8")

    error_response = {
        "Error": {
            "Code": "ServiceUnavailable",
            "Message": "Simulated S3 failure",
        }
    }

    error = ClientError(error_response, "PutObject")

    with patch(
        "src.s3_ingestion.s3.upload_file",
        side_effect=error,
    ):
        with pytest.raises(ClientError):
            upload_csv(csv_file, "bronze/orders/orders.csv")

def test_upload_calls_s3_and_verifies_metadata(tmp_path):
    
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text("order_id,amount\n101,500\n", encoding="utf-8")

    file_hash = calculate_sha256(csv_file)

    existing_metadata = {
        "Metadata": {},
    }

    uploaded_metadata = {
        "ContentLength": csv_file.stat().st_size,
        "ContentType": "text/csv",
        "Metadata": {"sha256": file_hash},
    }

    with (
        patch("src.s3_ingestion.s3.upload_file") as mock_upload,
        patch(
            "src.s3_ingestion.s3.head_object",
            side_effect=[existing_metadata, uploaded_metadata],
        ) as mock_head,
    ):
        upload_csv(csv_file, "bronze/orders/orders.csv")  

    mock_upload.assert_called_once_with(
        str(csv_file),
        "de-learning-lake",
        "bronze/orders/orders.csv",
        ExtraArgs={
            "ContentType": "text/csv",
            "Metadata": {"sha256": file_hash},
        },
    )

    assert mock_head.call_count == 2


def test_unchanged_file_skips_upload(tmp_path):

    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,amount\n101,500\n",
        encoding="utf-8",
    )

    file_hash = calculate_sha256(csv_file)

    existing_metadata = {
        "Metadata": {"sha256": file_hash},
    }

    with (
        patch(
            "src.s3_ingestion.s3.head_object",
            return_value=existing_metadata,
        ),
        patch("src.s3_ingestion.s3.upload_file") as mock_upload,
    ):
        upload_csv(csv_file, "bronze/orders/orders.csv")

    mock_upload.assert_not_called()

def test_changed_file_uploads_new_hash(tmp_path):
    

    # Arrange: create a changed CSV file
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,amount\n101,600\n",
        encoding="utf-8",
    )

    # Calculate the hash of the new file
    new_hash = calculate_sha256(csv_file)

    # Simulate an older version already stored in S3
    old_metadata = {
        "Metadata": {"sha256": "old-hash"},
    }

    # Simulate the metadata after the new file is uploaded
    updated_metadata = {
        "ContentLength": csv_file.stat().st_size,
        "ContentType": "text/csv",
        "Metadata": {"sha256": new_hash},
    }

    # Mock S3: first call returns old metadata, second returns updated metadata
    with (
        patch(
            "src.s3_ingestion.s3.head_object",
            side_effect=[old_metadata, updated_metadata],
        ) as mock_head,
        patch("src.s3_ingestion.s3.upload_file") as mock_upload,
    ):
        upload_csv(csv_file, "bronze/orders/orders.csv")

    # Verify that the changed file was uploaded exactly once
    mock_upload.assert_called_once_with(
        str(csv_file),
        "de-learning-lake",
        "bronze/orders/orders.csv",
        ExtraArgs={
            "ContentType": "text/csv",
            "Metadata": {"sha256": new_hash},
        },
    )

    # Verify that S3 metadata was checked before and after uploading
    assert mock_head.call_count == 2
    
def test_access_denied_does_not_upload_file(tmp_path):
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,amount\n101,500\n",
        encoding="utf-8",
    )

    access_denied = ClientError(
        {
            "Error": {
                "Code": "403",
                "Message": "Forbidden",
            }
        },
        "HeadObject",
    )

    with (
        patch(
            "src.s3_ingestion.s3.head_object",
            side_effect=access_denied,
        ),
        patch("src.s3_ingestion.s3.upload_file") as mock_upload,
    ):
        with pytest.raises(ClientError):
            upload_csv(csv_file, "bronze/orders/orders.csv")

    mock_upload.assert_not_called()
    
def test_missing_s3_object_is_uploaded(tmp_path):

    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,amount\n101,500\n",
        encoding="utf-8",
    )

    file_hash = calculate_sha256(csv_file)

    missing_object = ClientError(
        {
            "Error": {
                "Code": "404",
                "Message": "Not Found",
            }
        },
        "HeadObject",
    )

    uploaded_metadata = {
        "ContentLength": csv_file.stat().st_size,
        "ContentType": "text/csv",
        "Metadata": {"sha256": file_hash},
    }

    with (
        patch(
            "src.s3_ingestion.s3.head_object",
            side_effect=[missing_object, uploaded_metadata],
        ) as mock_head,
        patch("src.s3_ingestion.s3.upload_file") as mock_upload,
    ):
        upload_csv(csv_file, "bronze/orders/orders.csv")

    mock_upload.assert_called_once()
    assert mock_head.call_count == 2
    
from pathlib import Path
from unittest.mock import patch

from src.s3_ingestion import download_csv


def test_download_csv_when_local_file_missing(tmp_path):

    import hashlib

    destination = tmp_path / "incremental" / "orders_110_110.csv"

    content = (
        "order_id,customer_id,order_date,amount\n"
        "110,1,2026-11-12,500.00\n"
    )

    expected_hash = hashlib.sha256(
        content.encode("utf-8")
    ).hexdigest()

    def fake_download(bucket, key, filename):
        filename = Path(filename)
        print("Temporary path:", filename)
        print("Parent exists:", filename.parent.exists())
        print("File exists:", filename.exists())

        assert filename.parent.is_dir(), (
            f"Temporary directory does not exist: {filename.parent}"
        )

        with filename.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as file:
            file.write(content)

    with (
        patch(
            "src.s3_ingestion.s3.head_object",
            return_value={
                "Metadata": {
                    "sha256": expected_hash
                }
            },
        ) as mock_head,
        patch(
            "src.s3_ingestion.s3.download_file",
            side_effect=fake_download,
        ) as mock_download,
    ):

        result = download_csv(
            "bronze/orders/incremental/orders_110_110.csv",
            destination,
        )

    assert result == destination
    assert destination.is_file()

    mock_head.assert_called_once_with(
        Bucket=BUCKET_NAME,
        Key="bronze/orders/incremental/orders_110_110.csv",
    )

    mock_download.assert_called_once_with(
        BUCKET_NAME,
        "bronze/orders/incremental/orders_110_110.csv",
        str(mock_download.call_args.args[2]),
    )

    assert calculate_sha256(destination) == expected_hash
    
def test_download_csv_rejects_corrupted_file(tmp_path):

    from src.s3_ingestion import download_csv

    destination = tmp_path / "orders_110_110.csv"

    expected_content = (
        "order_id,customer_id,order_date,amount\n"
        "110,1,2026-11-12,500.00\n"
    )

    corrupted_content = (
        "order_id,customer_id,order_date,amount\n"
        "110,1,2026-11-12,999.00\n"
    )

    import hashlib

    expected_hash = hashlib.sha256(
        expected_content.encode("utf-8")
    ).hexdigest()

    with (
        patch(
            "src.s3_ingestion.s3.head_object",
            return_value={
                "Metadata": {"sha256": expected_hash}
            },
        ),
        patch(
            "src.s3_ingestion.s3.download_file",
            side_effect=lambda bucket, key, filename: (
                Path(filename).write_text(
                    corrupted_content,
                    encoding="utf-8",
                )
            ),
        ),
    ):

        with pytest.raises(
            RuntimeError,
            match="SHA-256 verification failed",
        ):
            download_csv(
                "bronze/orders/incremental/orders_110_110.csv",
                destination,
            )

    assert not destination.exists()