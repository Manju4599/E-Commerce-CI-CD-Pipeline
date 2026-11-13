
from pathlib import Path
from unittest.mock import patch, call

import pytest

from src.silver_recovery import recover_pending_silver_batches


def make_pending_batches():
    return [
        {
            "batch_id": "orders_109_110",
            "first_id": 109,
            "last_id": 110,
            "bronze_status": "completed",
            "silver_status": "pending",
        },
        {
            "batch_id": "orders_111_112",
            "first_id": 111,
            "last_id": 112,
            "bronze_status": "completed",
            "silver_status": "pending",
        },
    ]


def test_recovery_with_no_pending_batches():

    with (
        patch(
            "src.silver_recovery.get_pending_silver_batches",
            return_value=[],
        ),
        patch(
            "src.silver_recovery.upload_silver_orders"
        ) as mock_upload,
        patch(
            "src.silver_recovery.update_batch_status"
        ) as mock_update,
    ):

        result = recover_pending_silver_batches()

    assert result == []
    mock_upload.assert_not_called()
    mock_update.assert_not_called()


def test_recovery_processes_batches_and_uploads_once(tmp_path):

    pending_batches = make_pending_batches()

    incremental_dir = (
        tmp_path / "data" / "raw" / "incremental"
    )
    incremental_dir.mkdir(parents=True)

    for batch in pending_batches:
        batch_file = (
            incremental_dir / f"{batch['batch_id']}.csv"
        )
        batch_file.write_text("test data", encoding="utf-8")

    events = []

    def fake_transform(batch_file):
        events.append(f"transform:{Path(batch_file).stem}")

    def fake_upload():
        events.append("upload")

    def fake_update(batch_id, stage, status):
        events.append(f"update:{batch_id}:{stage}:{status}")
    
    def fake_download(s3_key, destination_path):
        assert destination_path is not None

        destination_path = Path(destination_path)
        assert destination_path.is_file()

        return destination_path
    with (
        patch(
            "src.silver_recovery.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.silver_recovery.get_pending_silver_batches",
            return_value=pending_batches,
        ),
        patch(
            "src.silver_recovery.transform_incremental_orders",
            side_effect=fake_transform,
        ) as mock_transform,
        patch(
            "src.silver_recovery.upload_silver_orders",
            side_effect=fake_upload,
        ) as mock_upload,
        patch(
            "src.silver_recovery.update_batch_status",
            side_effect=fake_update,
        ) as mock_update,
        patch(
            "src.silver_recovery.download_csv",
            side_effect=fake_download,) as mock_download,
    ):

        result = recover_pending_silver_batches()

    assert result == [
        "orders_109_110",
        "orders_111_112",
    ]

    assert mock_transform.call_count == 2
    mock_upload.assert_called_once()
    assert mock_update.call_count == 2

    # Verify that upload happens before batch completion
    assert events == [
        "transform:orders_109_110",
        "transform:orders_111_112",
        "upload",
        "update:orders_109_110:silver:completed",
        "update:orders_111_112:silver:completed",
    ]


def test_missing_bronze_batch_does_not_upload_silver(tmp_path):

    batch_id = "orders_110_110"

    with (
        patch(
            "src.silver_recovery.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.silver_recovery.get_pending_silver_batches",
            return_value=[
                {
                    "batch_id": batch_id,
                    "first_id": 110,
                    "last_id": 110,
                    "bronze_status": "completed",
                    "silver_status": "pending",
                }
            ],
        ),
        patch(
            "src.silver_recovery.download_csv",
            side_effect=RuntimeError("Bronze object not found"),
        ) as mock_download,
        patch(
            "src.silver_recovery.transform_incremental_orders",
        ) as mock_transform,
        patch(
            "src.silver_recovery.upload_silver_orders",
        ) as mock_upload,
        patch(
            "src.silver_recovery.update_batch_status",
        ) as mock_update,
    ):

        with pytest.raises(
            RuntimeError,
            match="Bronze object not found",
        ):
            recover_pending_silver_batches()

    mock_download.assert_called_once()
    mock_transform.assert_not_called()
    mock_upload.assert_not_called()
    mock_update.assert_not_called()


def test_silver_upload_failure_keeps_batches_pending(tmp_path):

    pending_batches = make_pending_batches()

    incremental_dir = (
        tmp_path / "data" / "raw" / "incremental"
    )
    incremental_dir.mkdir(parents=True)
    
    def fake_download(s3_key, destination_path):
        destination_path = Path(destination_path)
        assert destination_path.is_file()
        return destination_path

    for batch in pending_batches:
        (
            incremental_dir / f"{batch['batch_id']}.csv"
        ).write_text("test data", encoding="utf-8")

    with (
        patch(
            "src.silver_recovery.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.silver_recovery.download_csv",
        side_effect=fake_download,
        ),
        patch(
            "src.silver_recovery.get_pending_silver_batches",
            return_value=pending_batches,
        ),
        patch(
            "src.silver_recovery.transform_incremental_orders"
        ) as mock_transform,
        patch(
            "src.silver_recovery.upload_silver_orders",
            side_effect=RuntimeError("Silver upload failed"),
        ),
        patch(
            "src.silver_recovery.update_batch_status"
        ) as mock_update,
    ):

        with pytest.raises(
            RuntimeError,
            match="Silver upload failed",
        ):
            recover_pending_silver_batches()

    assert mock_transform.call_count == 2
    mock_update.assert_not_called()
    
def test_missing_local_bronze_batch_is_downloaded(tmp_path):

    batch_id = "orders_110_110"

    batch_file = (
        tmp_path
        / "data"
        / "raw"
        / "incremental"
        / f"{batch_id}.csv"
    )

    def fake_download(s3_key, destination_path):

        assert s3_key == (
            "bronze/orders/incremental/orders_110_110.csv"
        )

        destination_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        destination_path.write_text(
            "order_id,customer_id,order_date,amount\n"
            "110,1,2026-11-12,500.00\n",
            encoding="utf-8",
        )

        return destination_path

    with (
        patch(
            "src.silver_recovery.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.silver_recovery.get_pending_silver_batches",
            return_value=[
                {
                    "batch_id": batch_id,
                    "first_id": 110,
                    "last_id": 110,
                    "bronze_status": "completed",
                    "silver_status": "pending",
                }
            ],
        ),
        patch(
            "src.silver_recovery.download_csv",
            side_effect=fake_download,
        ) as mock_download,
        patch(
            "src.silver_recovery.transform_incremental_orders",
        ) as mock_transform,
        patch(
            "src.silver_recovery.upload_silver_orders",
        ) as mock_upload,
        patch(
            "src.silver_recovery.update_batch_status",
        ) as mock_update,
    ):

        result = recover_pending_silver_batches()

    assert result == [batch_id]

    assert batch_file.is_file()

    mock_download.assert_called_once()
    mock_transform.assert_called_once_with(batch_file)
    mock_upload.assert_called_once()

    mock_update.assert_called_once_with(
        batch_id,
        "silver",
        "completed",
    )
