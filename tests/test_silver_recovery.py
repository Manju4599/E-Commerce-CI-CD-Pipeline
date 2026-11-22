
from pathlib import Path
from unittest.mock import patch

import pandas as pd
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
            "src.silver_recovery.restore_committed_silver",
            return_value=False,
        ),
        patch(
            "src.silver_recovery.read_silver_manifest",
            return_value=None,
        ),
        patch(
            "src.silver_recovery.get_pending_silver_batches",
            return_value=[],
        ),
        patch(
            "src.silver_recovery.publish_versioned_silver",
        ) as mock_publish,
        patch(
            "src.silver_recovery.update_batch_status",
        ) as mock_update,
        patch(
            "src.silver_recovery.reconcile_batch_state",
            return_value=True,
        ),
    ):

        result = recover_pending_silver_batches()

    assert result == []
    mock_publish.assert_not_called()
    mock_update.assert_not_called()


def test_recovery_processes_batches_and_publishes_once(tmp_path):

    pending_batches = make_pending_batches()

    incremental_dir = (
        tmp_path / "data" / "raw" / "incremental"
    )
    incremental_dir.mkdir(parents=True)

    processed_dir = tmp_path / "data" / "processed"

    for batch in pending_batches:
        batch_file = (
            incremental_dir / f"{batch['batch_id']}.csv"
        )
        batch_file.write_text(
            "test data",
            encoding="utf-8",
        )

    events = []

    def fake_transform(batch_file):

        events.append(
            f"transform:{Path(batch_file).stem}"
        )

        processed_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_file = processed_dir / "orders_clean.csv"

        pd.DataFrame({
            "order_id": [110, 112],
            "customer_id": [1, 1],
            "order_date": [
                "2026-11-16",
                "2026-11-16",
            ],
            "amount": [1200, 1500],
        }).to_csv(output_file, index=False)

    def fake_publish(file_path, processed_batches):

        assert Path(file_path).is_file()
        assert len(processed_batches) == 2

        events.append("publish")

    def fake_update(batch_id, stage, status):

        events.append(
            f"update:{batch_id}:{stage}:{status}"
        )

    def fake_download(s3_key, destination_path):

        destination_path = Path(destination_path)

        assert destination_path.is_file()

        return destination_path

    with (
        patch(
            "src.silver_recovery.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.silver_recovery.PROCESSED_DIR",
            processed_dir,
        ),
        patch(
            "src.silver_recovery.restore_committed_silver",
            return_value=False,
        ),
        patch(
            "src.silver_recovery.read_silver_manifest",
            return_value=None,
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
            "src.silver_recovery.publish_versioned_silver",
            side_effect=fake_publish,
        ) as mock_publish,
        patch(
            "src.silver_recovery.update_batch_status",
            side_effect=fake_update,
        ) as mock_update,
        patch(
            "src.silver_recovery.download_csv",
            side_effect=fake_download,
        ) as mock_download,
        patch(
            "src.silver_recovery.reconcile_batch_state",
            return_value=True,
        ),
    ):

        result = recover_pending_silver_batches()

    assert result == [
        "orders_109_110",
        "orders_111_112",
    ]

    assert mock_transform.call_count == 2
    mock_publish.assert_called_once()
    assert mock_update.call_count == 2
    assert mock_download.call_count == 2

    assert events == [
        "transform:orders_109_110",
        "transform:orders_111_112",
        "publish",
        "update:orders_109_110:silver:completed",
        "update:orders_111_112:silver:completed",
    ]


def test_missing_bronze_batch_does_not_publish_silver(tmp_path):

    batch_id = "orders_110_110"
    processed_dir = tmp_path / "data" / "processed"
    with (
        patch(
            "src.silver_recovery.PROCESSED_DIR",
            processed_dir,
        ),
        patch(
            "src.silver_recovery.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.silver_recovery.restore_committed_silver",
            return_value=False,
        ),
        patch(
            "src.silver_recovery.read_silver_manifest",
            return_value=None,
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
            side_effect=RuntimeError(
                "Bronze object not found"
            ),
        ) as mock_download,
        patch(
            "src.silver_recovery.transform_incremental_orders",
        ) as mock_transform,
        patch(
            "src.silver_recovery.publish_versioned_silver",
        ) as mock_publish,
        patch(
            "src.silver_recovery.update_batch_status",
        ) as mock_update,
        patch(
            "src.silver_recovery.reconcile_batch_state",
            return_value=True,
        ),
    ):

        with pytest.raises(
            RuntimeError,
            match="Bronze object not found",
        ):
            recover_pending_silver_batches()
    mock_download.assert_called_once()
    mock_transform.assert_not_called()
    mock_publish.assert_not_called()
    mock_update.assert_not_called()


def test_silver_publish_failure_keeps_batches_pending(tmp_path):

    pending_batches = make_pending_batches()

    incremental_dir = (
        tmp_path / "data" / "raw" / "incremental"
    )
    incremental_dir.mkdir(parents=True)

    processed_dir = tmp_path / "data" / "processed"

    for batch in pending_batches:

        (
            incremental_dir / f"{batch['batch_id']}.csv"
        ).write_text(
            "test data",
            encoding="utf-8",
        )

    def fake_download(s3_key, destination_path):

        destination_path = Path(destination_path)

        assert destination_path.is_file()

        return destination_path

    def fake_transform(batch_file):
        processed_dir.mkdir(parents=True, exist_ok=True)

        pd.DataFrame({
            "order_id": [110],
            "customer_id": [1],
            "order_date": ["2026-11-12"],
            "amount": [500.00],
        }).to_csv(
            processed_dir / "orders_clean.csv",
            index=False,
        )

    with (
        patch(
            "src.silver_recovery.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.silver_recovery.PROCESSED_DIR",
            processed_dir,
        ),
        patch(
            "src.silver_recovery.restore_committed_silver",
            return_value=False,
        ),
        patch(
            "src.silver_recovery.read_silver_manifest",
            return_value=None,
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
            "src.silver_recovery.transform_incremental_orders",
            side_effect=fake_transform,
        ) as mock_transform,
        patch(
            "src.silver_recovery.publish_versioned_silver",
            side_effect=RuntimeError(
                "Silver publish failed"
            ),
        ),
        patch(
            "src.silver_recovery.update_batch_status",
        ) as mock_update,
        patch(
            "src.silver_recovery.reconcile_batch_state",
            return_value=True,
        ),
    ):

        with pytest.raises(
            RuntimeError,
            match="Silver publish failed",
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

    processed_dir = tmp_path / "data" / "processed"

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

    def fake_publish(file_path, processed_batches):
        assert Path(file_path).is_file()
        assert len(processed_batches) == 1
    
    def fake_transform(batch_file):
        processed_dir.mkdir(parents=True, exist_ok=True)

        pd.DataFrame({
            "order_id": [110],
            "customer_id": [1],
            "order_date": ["2026-11-12"],
            "amount": [500.00],
        }).to_csv(
            processed_dir / "orders_clean.csv",
            index=False,
        )
        
    processed_dir = tmp_path / "data" / "processed"
    
    with (
        patch(
            "src.silver_recovery.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.silver_recovery.PROCESSED_DIR",
            processed_dir,
        ),
        patch(
            "src.silver_recovery.restore_committed_silver",
            return_value=False,
        ),
        patch(
            "src.silver_recovery.read_silver_manifest",
            return_value=None,
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
            side_effect=fake_transform,
        ) as mock_transform,
        patch(
            "src.silver_recovery.publish_versioned_silver",
            side_effect=fake_publish,
        ) as mock_publish,
        patch(
            "src.silver_recovery.update_batch_status",
        ) as mock_update,
        patch(
            "src.silver_recovery.reconcile_batch_state",
            return_value=True,
        ),
    ):

        result = recover_pending_silver_batches()
        print("Expected batch path:", batch_file)
        print("Expected batch exists:", batch_file.exists())
        print("Recovery result:", result)
        print("Download calls:", mock_download.call_args_list)
        print("Transform calls:", mock_transform.call_args_list)

    assert result == [batch_id]
    assert batch_file.is_file()

    mock_download.assert_called_once_with("bronze/orders/incremental/orders_110_110.csv",
    batch_file,)    
    mock_transform.assert_called_once_with(batch_file)
    mock_publish.assert_called_once()

    mock_update.assert_called_once_with(
        batch_id,
        "silver",
        "completed",
    )


def test_recovery_repairs_ledger_after_manifest_commit():

    batch_id = "orders_113_114"

    pending_batch = {
        "batch_id": batch_id,
        "first_id": 113,
        "last_id": 114,
        "bronze_status": "completed",
        "silver_status": "pending",
    }

    committed_manifest = {
        "silver_version": 3,
        "file": "silver/orders/orders_v003.csv",
        "sha256": "a" * 64,
        "last_processed_order_id": 114,
        "committed_batches": [batch_id],
    }

    with (
        patch(
            "src.silver_recovery.restore_committed_silver",
            return_value=True,
        ),
        patch(
            "src.silver_recovery.read_silver_manifest",
            return_value=committed_manifest,
        ),
        patch(
            "src.silver_recovery.get_pending_silver_batches",
            return_value=[pending_batch],
        ),
        patch(
            "src.silver_recovery.transform_incremental_orders",
        ) as mock_transform,
        patch(
            "src.silver_recovery.publish_versioned_silver",
        ) as mock_publish,
        patch(
            "src.silver_recovery.download_csv",
        ) as mock_download,
        patch(
            "src.silver_recovery.update_batch_status",
        ) as mock_update,
        patch(
            "src.silver_recovery.reconcile_batch_state",
            return_value=True,
        ),
    ):

        result = recover_pending_silver_batches()

    assert result == [batch_id]

    # Already committed: no reprocessing or republishing.
    mock_transform.assert_not_called()
    mock_publish.assert_not_called()
    mock_download.assert_not_called()

    # Repair the batch ledger.
    mock_update.assert_called_once_with(
        batch_id,
        "silver",
        "completed",
    )

def test_failed_silver_publication_preserves_persistent_pending_batch(
    tmp_path, monkeypatch
):
    import json

    from src.batch_state import (
        get_pending_silver_batches,
        load_batch_state,
        register_batch,
        update_batch_status,
    )

    batch_state_file = tmp_path / "batches.json"
    monkeypatch.setattr(
        "src.batch_state.BATCH_STATE_FILE",
        batch_state_file,
    )

    # Create a real persistent Bronze-completed batch.
    register_batch(111, 112)
    update_batch_status(
        "orders_111_112",
        "bronze",
        "completed",
    )

    pending_before = get_pending_silver_batches()

    assert len(pending_before) == 1
    assert pending_before[0]["batch_id"] == "orders_111_112"

    incremental_dir = (
        tmp_path / "data" / "raw" / "incremental"
    )
    incremental_dir.mkdir(parents=True)

    batch_file = incremental_dir / "orders_111_112.csv"
    batch_file.write_text(
        "order_id,customer_id,order_date,amount\n"
        "111,1,2026-11-20,500.00\n"
        "112,2,2026-11-20,750.00\n",
        encoding="utf-8",
    )

    processed_dir = tmp_path / "data" / "processed"

    def fake_transform(file_path):
        processed_dir.mkdir(parents=True, exist_ok=True)

        pd.DataFrame({
            "order_id": [111, 112],
            "customer_id": [1, 2],
            "order_date": ["2026-11-20", "2026-11-20"],
            "amount": [500.00, 750.00],
        }).to_csv(
            processed_dir / "orders_clean.csv",
            index=False,
        )

    with (
        patch(
            "src.silver_recovery.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.silver_recovery.PROCESSED_DIR",
            processed_dir,
        ),
        patch(
            "src.silver_recovery.restore_committed_silver",
            return_value=False,
        ),
        patch(
            "src.silver_recovery.read_silver_manifest",
            return_value=None,
        ),
        patch(
            "src.silver_recovery.download_csv",
        ),
        patch(
            "src.silver_recovery.transform_incremental_orders",
            side_effect=fake_transform,
        ),
        patch(
            "src.silver_recovery.publish_versioned_silver",
            side_effect=RuntimeError("Simulated Silver failure"),
        ),
    ):
        with pytest.raises(
            RuntimeError,
            match="Simulated Silver failure",
        ):
            recover_pending_silver_batches()

    # Inspect the actual persisted ledger after the failure.
    state_after_failure = load_batch_state()

    assert len(state_after_failure["batches"]) == 1
    assert state_after_failure["batches"][0]["bronze_status"] == (
        "completed"
    )
    assert state_after_failure["batches"][0]["silver_status"] == (
        "pending"
    )

    pending_after = get_pending_silver_batches()

    assert len(pending_after) == 1
    assert pending_after[0]["batch_id"] == "orders_111_112"


def test_recovery_repairs_persistent_ledger_after_manifest_commit(
    tmp_path, monkeypatch
):
    import json

    from src.batch_state import (
        load_batch_state,
        register_batch,
        update_batch_status,
    )

    batch_state_file = tmp_path / "batches.json"

    monkeypatch.setattr(
        "src.batch_state.BATCH_STATE_FILE",
        batch_state_file,
    )

    batch_id = "orders_111_112"

    # Create a persistent Bronze-completed, Silver-pending batch.
    register_batch(111, 112)
    update_batch_status(
        batch_id,
        "bronze",
        "completed",
    )

    committed_manifest = {
        "silver_version": 2,
        "file": "silver/orders/orders_v002.csv",
        "sha256": "a" * 64,
        "last_processed_order_id": 112,
        "committed_batches": [batch_id],
    }

    with (
        patch(
            "src.silver_recovery.PROCESSED_DIR",
            tmp_path / "data" / "processed",
        ),
        patch(
            "src.silver_recovery.restore_committed_silver",
            return_value=True,
        ) as mock_restore,
        patch(
            "src.silver_recovery.read_silver_manifest",
            return_value=committed_manifest,
        ),
        patch(
            "src.silver_recovery.verify_silver_artifact",
            return_value=True,
        ),
        patch(
            "src.silver_recovery.get_pending_silver_batches",
        ) as mock_pending,
        patch(
            "src.silver_recovery.transform_incremental_orders",
        ) as mock_transform,
        patch(
            "src.silver_recovery.publish_versioned_silver",
        ) as mock_publish,
        patch(
            "src.batch_reconciliation.read_silver_manifest",
            return_value=committed_manifest,
        ),
        patch(
            "src.batch_reconciliation.verify_silver_artifact",
            return_value=True,
        ),
    ):
        from src.batch_state import get_pending_silver_batches

        mock_pending.side_effect = get_pending_silver_batches

        result = recover_pending_silver_batches()

    assert result == [batch_id]

    # No transformation or republishing of committed data.
    mock_transform.assert_not_called()
    mock_publish.assert_not_called()

    # The persistent ledger must now be repaired.
    state = load_batch_state()

    assert len(state["batches"]) == 1
    assert state["batches"][0]["bronze_status"] == "completed"
    assert state["batches"][0]["silver_status"] == "completed"

    mock_restore.assert_called_once()
