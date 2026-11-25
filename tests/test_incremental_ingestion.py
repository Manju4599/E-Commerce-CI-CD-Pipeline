import pandas as pd
import pytest
from unittest.mock import patch
import json
from src.incremental_ingestion import ingest_incremental_orders
from unittest.mock import patch, call
from src.batch_state import update_batch_status

def make_batch():
    return pd.DataFrame({
        "order_id": [104, 105, 106],
        "customer_id": [1, 2, 3],
        "amount": [500.00, 750.00, 900.00],
    })


def test_successful_upload_commits_watermark(tmp_path):

    batch = make_batch()
    events = []

    def fake_recovery():
        events.append("recovery")

    def fake_extract():
        events.append("extract")
        return batch, 106

    def fake_bronze_upload(file_path, s3_key):
        events.append("bronze")

    def fake_register(first_id, last_id):
        events.append("register")

    def fake_update(batch_id, stage, status):
        events.append("bronze_completed")

    def fake_save(table_name, last_id):
        events.append("save")

    with (
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
            side_effect=fake_recovery,
        ) as mock_recovery,
        patch(
            "src.incremental_ingestion.extract_new_orders",
            side_effect=fake_extract,
        ),
        patch(
            "src.incremental_ingestion.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.incremental_ingestion.upload_csv",
            side_effect=fake_bronze_upload,
        ) as mock_bronze,
        patch(
            "src.incremental_ingestion.register_batch",
            side_effect=fake_register,
        ) as mock_register,
        patch(
            "src.incremental_ingestion.update_batch_status",
            side_effect=fake_update,
        ) as mock_update,
        patch(
            "src.incremental_ingestion.save_watermark",
            side_effect=fake_save,
        ) as mock_save,
    ):

        result = ingest_incremental_orders()

    expected_file = (
        tmp_path
        / "data"
        / "raw"
        / "incremental"
        / "orders_104_106.csv"
    )

    assert result == 106

    mock_bronze.assert_called_once_with(
        expected_file,
        "bronze/orders/incremental/orders_104_106.csv",
    )

    mock_register.assert_called_once_with(104, 106)

    mock_update.assert_called_once_with(
        "orders_104_106",
        "bronze",
        "completed",
    )

    mock_save.assert_called_once_with(
        "orders_bronze",
        106,
    )

    assert mock_recovery.call_count == 2

    assert events == [
        "recovery",
        "extract",
        "bronze",
        "register",
        "bronze_completed",
        "save",
        "recovery",
    ]

def test_bronze_upload_failure_does_not_commit_watermark(tmp_path):

    batch = make_batch()

    with (
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value=(batch, 106),
        ),
        patch(
            "src.incremental_ingestion.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.incremental_ingestion.upload_csv",
            side_effect=RuntimeError("Simulated S3 failure"),
        ),
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
        ) as mock_recovery,
        patch(
            "src.incremental_ingestion.register_batch",
        ) as mock_register,
        patch(
            "src.incremental_ingestion.update_batch_status",
        ) as mock_update,
        patch(
            "src.incremental_ingestion.save_watermark",
        ) as mock_save,
    ):

        with pytest.raises(
            RuntimeError,
            match="Simulated S3 failure",
        ):
            ingest_incremental_orders()

    mock_recovery.assert_called_once_with()
    mock_register.assert_not_called()
    mock_update.assert_not_called()
    mock_save.assert_not_called()


def test_no_new_orders_does_not_upload_or_commit():

    empty_batch = pd.DataFrame(
        columns=[
            "order_id",
            "customer_id",
            "order_date",
            "amount",
        ]
    )

    with (
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value=(empty_batch, 106),
        ),
        patch(
            "src.incremental_ingestion.upload_csv",
        ) as mock_bronze,
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
        ) as mock_recovery,
        patch(
            "src.incremental_ingestion.register_batch",
        ) as mock_register,
        patch(
            "src.incremental_ingestion.update_batch_status",
        ) as mock_update,
        patch(
            "src.incremental_ingestion.save_watermark",
        ) as mock_save,
    ):

        result = ingest_incremental_orders()

    assert result == 106

    mock_recovery.assert_called_once_with()
    mock_bronze.assert_not_called()
    mock_register.assert_not_called()
    mock_update.assert_not_called()
    mock_save.assert_not_called()


def test_failed_batch_can_be_retried_safely(tmp_path):

    batch = make_batch()

    with (
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value=(batch, 106),
        ),
        patch(
            "src.incremental_ingestion.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.incremental_ingestion.upload_csv",
            side_effect=[
                RuntimeError("Temporary S3 failure"),
                None,
            ],
        ) as mock_bronze,
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
        ) as mock_recovery,
        patch(
            "src.incremental_ingestion.register_batch",
        ) as mock_register,
        patch(
            "src.incremental_ingestion.update_batch_status",
        ) as mock_update,
        patch(
            "src.incremental_ingestion.save_watermark",
        ) as mock_save,
    ):

        # First attempt: Bronze upload fails.
        with pytest.raises(
            RuntimeError,
            match="Temporary S3 failure",
        ):
            ingest_incremental_orders()

        mock_save.assert_not_called()
        mock_register.assert_not_called()
        mock_update.assert_not_called()

        # Second attempt: Bronze upload succeeds.
        result = ingest_incremental_orders()

    assert result == 106

    assert mock_bronze.call_count == 2

    # Both attempts target the same Bronze object.
    assert (
        mock_bronze.call_args_list[0]
        == mock_bronze.call_args_list[1]
    )

    # Two initial recoveries and one post-upload recovery.
    assert mock_recovery.call_count == 3

    mock_register.assert_called_once_with(104, 106)

    mock_update.assert_called_once_with(
        "orders_104_106",
        "bronze",
        "completed",
    )

    mock_save.assert_called_once_with(
        "orders_bronze",
        106,
    )

def test_silver_transformation_failure_commits_bronze_watermark(
    tmp_path,
):

    batch = make_batch()

    with (
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value=(batch, 106),
        ),
        patch(
            "src.incremental_ingestion.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.incremental_ingestion.upload_csv",
        ) as mock_bronze,
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
            side_effect=[
                None,
                RuntimeError("Transformation failed"),
            ],
        ) as mock_recovery,
        patch(
            "src.incremental_ingestion.register_batch",
        ) as mock_register,
        patch(
            "src.incremental_ingestion.update_batch_status",
        ) as mock_update,
        patch(
            "src.incremental_ingestion.save_watermark",
        ) as mock_save,
    ):

        with pytest.raises(
            RuntimeError,
            match="Transformation failed",
        ):
            ingest_incremental_orders()

    mock_bronze.assert_called_once()

    mock_register.assert_called_once_with(104, 106)

    mock_update.assert_called_once_with(
        "orders_104_106",
        "bronze",
        "completed",
    )

    mock_save.assert_called_once_with(
        "orders_bronze",
        106,
    )

    assert mock_recovery.call_count == 2


def test_silver_upload_failure_commits_bronze_watermark(
    tmp_path,
):

    batch = make_batch()

    with (
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value=(batch, 106),
        ),
        patch(
            "src.incremental_ingestion.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.incremental_ingestion.upload_csv",
        ) as mock_bronze,
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
            side_effect=[
                None,
                RuntimeError("Silver S3 upload failed"),
            ],
        ) as mock_recovery,
        patch(
            "src.incremental_ingestion.register_batch",
        ) as mock_register,
        patch(
            "src.incremental_ingestion.update_batch_status",
        ) as mock_update,
        patch(
            "src.incremental_ingestion.save_watermark",
        ) as mock_save,
    ):

        with pytest.raises(
            RuntimeError,
            match="Silver S3 upload failed",
        ):
            ingest_incremental_orders()

    mock_bronze.assert_called_once()

    mock_register.assert_called_once_with(104, 106)

    mock_update.assert_called_once_with(
        "orders_104_106",
        "bronze",
        "completed",
    )

    mock_save.assert_called_once_with(
        "orders_bronze",
        106,
    )

    assert mock_recovery.call_count == 2



def test_failed_atomic_write_preserves_existing_silver(tmp_path):

    from src.transform import merge_incremental_orders

    output_path = tmp_path / "orders_clean.csv"

    existing = pd.DataFrame({
        "order_id": [101, 102],
        "customer_id": [1, 2],
        "order_date": ["2026-11-01", "2026-11-02"],
        "amount": [500.00, 750.00]
    })

    existing.to_csv(output_path, index=False)

    new_orders = pd.DataFrame({
        "order_id": [103],
        "customer_id": [3],
        "order_date": ["2026-11-03"],
        "amount": [900.00]
    })

    with patch(
        "src.transform.PROCESSED_DIR",
        tmp_path
    ):
        with patch(
            "src.transform.pd.DataFrame.to_csv",
            side_effect=OSError("Simulated disk failure")
        ):
            with pytest.raises(
                OSError,
                match="Simulated disk failure"
            ):
                merge_incremental_orders(new_orders)

    # Original Silver file must remain unchanged
    saved = pd.read_csv(output_path)

    assert len(saved) == 2
    assert saved["order_id"].tolist() == [101, 102]

    # Temporary file should be cleaned up
    temp_files = list(tmp_path.glob("*.tmp"))
    hidden_temp_files = list(tmp_path.glob(".*.tmp"))

    assert len(temp_files) + len(hidden_temp_files) == 0

def test_watermark_failure_after_bronze_upload_preserves_batch_for_retry(
    tmp_path,
):
    batch = make_batch()
    events = []

    def fake_upload(file_path, s3_key):
        events.append("bronze_upload")

    def fake_register(first_id, last_id):
        events.append("register")

    def fake_update(batch_id, stage, status):
        events.append("bronze_completed")

    def fake_save(table_name, last_id):
        events.append("watermark_save")
        raise RuntimeError("Simulated watermark failure")

    with (
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value=(batch, 106),
        ),
        patch(
            "src.incremental_ingestion.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
        ) as mock_recovery,
        patch(
            "src.incremental_ingestion.upload_csv",
            side_effect=fake_upload,
        ),
        patch(
            "src.incremental_ingestion.register_batch",
            side_effect=fake_register,
        ),
        patch(
            "src.incremental_ingestion.update_batch_status",
            side_effect=fake_update,
        ),
        patch(
            "src.incremental_ingestion.save_watermark",
            side_effect=fake_save,
        ) as mock_save,
    ):
        with pytest.raises(
            RuntimeError,
            match="Simulated watermark failure",
        ):
            ingest_incremental_orders()

    assert events == [
        "bronze_upload",
        "register",
        "bronze_completed",
        "watermark_save",
    ]

    mock_save.assert_called_once_with("orders_bronze", 106)

    # Recovery must not run after the watermark failure.
    mock_recovery.assert_called_once_with()


def test_retry_after_watermark_failure_preserves_persistent_batch_state(
    tmp_path, monkeypatch
):


    from src.batch_state import load_batch_state
    from src.watermark import (
        load_watermark,
        save_watermark as persist_watermark,
    )

    batch_state_file = tmp_path / "batches.json"
    watermark_file = tmp_path / "watermarks.json"

    watermark_file.write_text(
        json.dumps({"orders_bronze": 110}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "src.batch_state.BATCH_STATE_FILE",
        batch_state_file,
    )
    monkeypatch.setattr(
        "src.watermark.WATERMARK_FILE",
        watermark_file,
    )

    batch = pd.DataFrame({
        "order_id": [111],
        "customer_id": [1],
        "order_date": ["2026-11-20"],
        "amount": [1200.00],
    })

    upload_calls = []
    recovery_calls = []
    watermark_attempts = 0

    def fake_upload(file_path, s3_key):
        upload_calls.append(s3_key)

    def fake_save_watermark(table_name, last_id):
        nonlocal watermark_attempts
        watermark_attempts += 1

        if watermark_attempts == 1:
            raise RuntimeError("Simulated watermark failure")

        persist_watermark(table_name, last_id)

    with (
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value=(batch, 111),
        ),
        patch(
            "src.incremental_ingestion.PROJECT_ROOT",
            tmp_path,
        ),
        patch(
            "src.incremental_ingestion.upload_csv",
            side_effect=fake_upload,
        ),
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
            side_effect=lambda: recovery_calls.append("recovery"),
        ),
        patch(
            "src.incremental_ingestion.save_watermark",
            side_effect=fake_save_watermark,
        ),
    ):
        # First attempt: Bronze and ledger succeed; watermark fails.
        with pytest.raises(
            RuntimeError,
            match="Simulated watermark failure",
        ):
            ingest_incremental_orders()

        # Verify persistent state after the failure.
        state_after_failure = load_batch_state()

        assert state_after_failure["batches"][0]["batch_id"] == (
            "orders_111_111"
        )
        assert state_after_failure["batches"][0]["bronze_status"] == (
            "completed"
        )
        assert state_after_failure["batches"][0]["silver_status"] == (
            "pending"
        )
        assert load_watermark("orders_bronze") == 110

        # Second attempt: same batch is retried successfully.
        result = ingest_incremental_orders()

    assert result == 111
    assert load_watermark("orders_bronze") == 111

    final_state = load_batch_state()
    assert len(final_state["batches"]) == 1
    assert final_state["batches"][0]["batch_id"] == "orders_111_111"
    assert final_state["batches"][0]["bronze_status"] == "completed"
    assert final_state["batches"][0]["silver_status"] == "pending"

    # Both attempts target the same Bronze object.
    assert upload_calls == [
        "bronze/orders/incremental/orders_111_111.csv",
        "bronze/orders/incremental/orders_111_111.csv",
    ]

    # Initial recovery on both attempts, plus recovery after successful retry.
    assert len(recovery_calls) == 3
    assert watermark_attempts == 2

def test_integration_no_new_orders_does_not_upload_or_commit(tmp_path):

    empty_batch = pd.DataFrame(
        columns=[
                    "order_id",
                    "customer_id",
                    "order_date",
                    "amount",
        ]
    )

    # Create integration configuration.
    config = {
        "watermark_file": tmp_path / "state" / "watermarks.json",
        "state_file": tmp_path / "state" / "batches.json",
        "incremental_dir": tmp_path / "raw" / "incremental",
        "processed_dir": tmp_path / "processed",
        "customers_file": tmp_path / "customers.csv",
        "bronze_prefix":  "test/bronze",
        "manifest_key": "test/silver/manifest.json",
        "silver_prefix": "test/silver",
    }

    # Mock extraction.
    # Return empty_batch and candidate watermark 112.
    with (
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value=(empty_batch, 112),
        ) as mock_extract,
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
        ) as mock_recovery,
        patch(
            "src.incremental_ingestion.upload_csv"
        ) as mock_upload,
        patch(
            "src.incremental_ingestion.register_batch"
        ) as mock_register,
        patch(
            "src.incremental_ingestion.update_batch_status"
        ) as mock_update,
        patch(
            "src.incremental_ingestion.save_watermark"
        ) as mock_save,
    ):

        result = ingest_incremental_orders(config)

    assert result == 112

    mock_extract.assert_called_once_with(
        watermark_file=config["watermark_file"],
        output_dir=config["incremental_dir"],
    )

    recovery_config = {
        "state_file": config["state_file"],
        "incremental_dir": config["incremental_dir"],
        "processed_dir": config["processed_dir"],
        "customers_file": config["customers_file"],
        "bronze_prefix": config["bronze_prefix"],
        "manifest_key": config["manifest_key"],
        "silver_prefix": config["silver_prefix"],
    }

    mock_recovery.assert_called_once_with(**recovery_config)

    mock_upload.assert_not_called()
    mock_register.assert_not_called()
    mock_update.assert_not_called()
    mock_save.assert_not_called()

def test_integration_successful_upload_commits_watermark(tmp_path):
    orders_df = pd.DataFrame({
                    "order_id":[113,114],
                    "customer_id":[7,4],
                    "order_date":["2026-11-23","2026-11-23"],
                    "amount":[500,950],
                })
    config = {
        "watermark_file": tmp_path / "state" / "watermarks.json",
        "state_file": tmp_path / "state" / "batches.json",
        "incremental_dir": tmp_path / "raw" / "incremental",
        "processed_dir": tmp_path / "processed",
        "customers_file": tmp_path / "customers.csv",
        "bronze_prefix": "test/bronze",
        "manifest_key": "test/silver/manifest.json",
        "silver_prefix": "test/silver",
    }

    recovery_config = {
         "state_file": config["state_file"],
        "incremental_dir": config["incremental_dir"],
        "processed_dir": config["processed_dir"],
        "customers_file": config["customers_file"],
        "bronze_prefix": config["bronze_prefix"],
        "manifest_key": config["manifest_key"],
        "silver_prefix": config["silver_prefix"],
    }

    with(
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value = (orders_df, 114),
        ) as mock_extract,
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
        ) as mock_recovery,

        patch(
            "src.incremental_ingestion.upload_csv",
        ) as mock_upload,

        patch(
            "src.incremental_ingestion.register_batch",
        ) as mock_register,

        patch(
            "src.incremental_ingestion.update_batch_status",
        ) as mock_update,

        patch(
            "src.incremental_ingestion.save_watermark",
        ) as mock_save,
    ):
        result = ingest_incremental_orders(config)

    assert result == 114
    mock_extract.assert_called_once_with(
        watermark_file = config["watermark_file"],
        output_dir = config["incremental_dir"],
    )
    assert mock_recovery.call_count == 2
    mock_recovery.assert_has_calls([
        call(**recovery_config),
        call(**recovery_config),
    ])
    expected_batch_file = config["incremental_dir"] / "orders_113_114.csv"
    expected_s3_key = f"{config['bronze_prefix']}/orders_113_114.csv"

    mock_upload.assert_called_once_with(expected_batch_file, expected_s3_key)

    expected_first_id = int(orders_df["order_id"].min())
    expected_last_id = int(orders_df["order_id"].max())
    mock_register.assert_called_once_with(expected_first_id, expected_last_id, config["state_file"])

    batch_id = "orders_113_114"
    mock_update.assert_called_once_with(batch_id,"bronze","completed",config["state_file"])

    mock_save.assert_called_once_with("orders_bronze",114,config["watermark_file"])


def test_integration_watermark_failure_preserves_batch_for_retry(tmp_path):
    orders_df = pd.DataFrame({
                    "order_id":[113,114],
                    "customer_id":[7,4],
                    "order_date":["2026-11-23","2026-11-23"],
                    "amount":[500,950],
                })

    config = {
        "watermark_file": tmp_path / "state" / "watermarks.json",
        "state_file": tmp_path / "state" / "batches.json",
        "incremental_dir": tmp_path / "raw" / "incremental",
        "processed_dir": tmp_path / "processed",
        "customers_file": tmp_path / "customers.csv",
        "bronze_prefix": "test/bronze",
        "manifest_key": "test/silver/manifest.json",
        "silver_prefix": "test/silver",
    }
    recovery_config = {
        "state_file": config["state_file"],
        "incremental_dir": config["incremental_dir"],
        "processed_dir": config["processed_dir"],
        "customers_file": config["customers_file"],
        "bronze_prefix": config["bronze_prefix"],
        "manifest_key": config["manifest_key"],
        "silver_prefix": config["silver_prefix"],
    }
    config["watermark_file"].parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    config["watermark_file"].write_text(
        json.dumps({"orders_bronze": 112}),
        encoding="utf-8",
    )

    with(
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value = (orders_df, 114),
        ) as mock_extract,
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches"
        ) as mock_recovery,
         patch(
            "src.incremental_ingestion.upload_csv",
        ) as mock_upload,
         patch(
             "src.incremental_ingestion.save_watermark",
             side_effect = RuntimeError("Simulated Watermark Failure"),
         ) as mock_save,
    ):
        with pytest.raises(RuntimeError, match ="Simulated Watermark Failure"):
            ingest_incremental_orders(config)

        watermark_data = json.loads(
        config["watermark_file"].read_text()
        )

        assert watermark_data["orders_bronze"] == 112

    expected_batch_file = (
    config["incremental_dir"] / "orders_113_114.csv"
    )

    mock_recovery.assert_called_once_with(**recovery_config)
    expected_s3_key = "test/bronze/orders_113_114.csv"

    mock_upload.assert_called_once_with(
        expected_batch_file,
        expected_s3_key,
    )
    mock_save.assert_called_once_with(
        "orders_bronze",
        114,
        config["watermark_file"],
    )


    assert config["state_file"].exists()
    assert mock_recovery.call_count == 1

    state_data = json.loads(config["state_file"].read_text())
    batch = state_data["batches"][0]
    assert batch["batch_id"] == "orders_113_114"
    assert batch["bronze_status"] == "completed"
    assert batch["silver_status"] == "pending"
    assert len(state_data["batches"]) == 1
    assert batch["first_id"] == 113
    assert batch["last_id"] == 114

def test_integration_retry_after_watermark_failure(tmp_path):
    orders_df = pd.DataFrame({
        "order_id" : [113,114],
        "customer_id" : [6,5],
        "order_date" : ["2026-11-25","2026-11-25"],
        "amount" : [500,700],
    })
    config = {
            "watermark_file": tmp_path / "state" / "watermarks.json",
            "state_file": tmp_path / "state" / "batches.json",
            "incremental_dir": tmp_path / "raw" / "incremental",
            "processed_dir": tmp_path / "processed",
            "customers_file": tmp_path / "customers.csv",
            "bronze_prefix": "test/bronze",
            "manifest_key": "test/silver/manifest.json",
            "silver_prefix": "test/silver",
    }
    recovery_config = {
            "state_file": config["state_file"],
            "incremental_dir": config["incremental_dir"],
            "processed_dir": config["processed_dir"],
            "customers_file": config["customers_file"],
            "bronze_prefix": config["bronze_prefix"],
            "manifest_key": config["manifest_key"],
            "silver_prefix": config["silver_prefix"],
    }

    config["watermark_file"].parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    config["watermark_file"].write_text(
        json.dumps({"orders_bronze": 112}),
        encoding="utf-8",
    )

    config["state_file"].parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with(
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value = (orders_df, 114)
        ) as mock_extract,
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
        ) as mock_recover,
        patch(
            "src.incremental_ingestion.upload_csv",
        ) as mock_upload,
        patch(
            "src.incremental_ingestion.save_watermark",
            side_effect = RuntimeError("Simulated Watermark Failure")
        ),
    ):
        with pytest.raises(RuntimeError, match="Simulated Watermark Failure"):
            ingest_incremental_orders(config)

        watermark_data = json.loads(config["watermark_file"].read_text())
        assert watermark_data["orders_bronze"] == 112

        expected_batch_file = config["incremental_dir"] / "orders_113_114.csv"
        expected_s3_key = f"{config['bronze_prefix']}/orders_113_114.csv"

        mock_extract.assert_called_once_with(
            watermark_file=config["watermark_file"],
            output_dir=config["incremental_dir"],
        )
        mock_recover.assert_called_once_with(**recovery_config)
        mock_upload.assert_called_once_with(expected_batch_file, expected_s3_key)

        state_data = json.loads(config["state_file"].read_text())
        assert len(state_data["batches"]) == 1
        assert state_data["batches"][0]["batch_id"] == "orders_113_114"
        assert state_data["batches"][0]["bronze_status"] == "completed"

    with(
        patch(
            "src.incremental_ingestion.extract_new_orders",
            return_value = (orders_df, 114),
        ) as mock_extract_n,
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
        ) as mock_recover_n,
        patch(
            "src.incremental_ingestion.upload_csv",
        ) as mock_upload_n,

    ):
        ingest_incremental_orders(config)

        mock_extract_n.assert_called_once_with(
            watermark_file=config["watermark_file"],
            output_dir=config["incremental_dir"],
        )
        mock_recover_n.assert_has_calls([
            call(**recovery_config),
            call(**recovery_config),
        ])
        assert mock_recover_n.call_count == 2
        mock_upload_n.assert_called_once_with(expected_batch_file, expected_s3_key)

    final_watermark = json.loads(config["watermark_file"].read_text())
    assert final_watermark["orders_bronze"] == 114

    final_state = json.loads(config["state_file"].read_text())
    assert len(final_state["batches"]) == 1

    assert final_state["batches"][0]["batch_id"] == "orders_113_114"
    assert final_state["batches"][0]["bronze_status"] == "completed"
    assert final_state["batches"][0]["first_id"] == 113
    assert final_state["batches"][0]["last_id"] == 114
    assert final_state["batches"][0]["silver_status"] == "pending"

def test_integration_repeated_run_does_not_create_duplicate_batch(tmp_path):
    orders_df = pd.DataFrame({
        "order_id":[113,114],
        "customer_id":[4,7],
        "order_date":["2026-11-25","2026-11-25"],
        "amount":[450,299],
    })
    empty_orders = pd.DataFrame(columns=[
                        "order_id",
                        "customer_id",
                        "order_date",
                        "amount",
    ])
    config = {
                "watermark_file": tmp_path / "state" / "watermarks.json",
                "state_file": tmp_path / "state" / "batches.json",
                "incremental_dir": tmp_path / "raw" / "incremental",
                "processed_dir": tmp_path / "processed",
                "customers_file": tmp_path / "customers.csv",
                "bronze_prefix": "test/bronze",
                "manifest_key": "test/silver/manifest.json",
                "silver_prefix": "test/silver",
    }
    recovery_config = {
            "state_file": config["state_file"],
            "incremental_dir": config["incremental_dir"],
            "processed_dir": config["processed_dir"],
            "customers_file": config["customers_file"],
            "bronze_prefix": config["bronze_prefix"],
            "manifest_key": config["manifest_key"],
            "silver_prefix": config["silver_prefix"],
    }

    config["watermark_file"].parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    config["watermark_file"].write_text(
        json.dumps({"orders_bronze": 112}),
        encoding="utf-8",
    )

    config["state_file"].parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    expected_batch_file = config["incremental_dir"] / "orders_113_114.csv"
    expected_s3_key = f"{config['bronze_prefix']}/orders_113_114.csv"

    s3_manifests = {}
    def fake_recovery(**kwargs):

        state_file = kwargs["state_file"]
        manifest_key = kwargs["manifest_key"]

        if not state_file.exists():
            return

        state = json.loads(
            state_file.read_text(encoding="utf-8")
        )

        manifest = s3_manifests.setdefault(
            manifest_key,
            {
                "silver_version": 0,
                "file": None,
                "sha256": "dummy_sha256",
                "last_processed_order_id": 112,
                "committed_batches": [],
            },
        )

        for batch in state["batches"]:

            if batch["silver_status"] == "pending":

                update_batch_status(
                    batch["batch_id"],
                    "silver",
                    "completed",
                    state_file,
                )

                manifest["silver_version"] += 1

                manifest["file"] = (
                    f"{kwargs['silver_prefix']}/orders/"
                    f"orders_v{manifest['silver_version']:03d}.csv"
                )

                manifest["last_processed_order_id"] = batch["last_id"]

                manifest["committed_batches"].append(
                    batch["batch_id"]
                )

    with(
        patch(
            "src.incremental_ingestion.extract_new_orders",
            side_effect = [
                (orders_df, 114),
                (empty_orders, 114),
            ]
        ) as mock_extract,
        patch(
            "src.incremental_ingestion.recover_pending_silver_batches",
            side_effect = fake_recovery,
        ) as mock_recovery,
        patch(
            "src.incremental_ingestion.upload_csv"
        ) as mock_upload,
    ):
        first_result = ingest_incremental_orders(config)
        second_result = ingest_incremental_orders(config)

    assert first_result == 114
    assert second_result == 114
    assert mock_extract.call_count == 2

    mock_recovery.assert_has_calls([
        call(**recovery_config),
        call(**recovery_config),
        call(**recovery_config),])
    assert mock_recovery.call_count == 3


    mock_upload.assert_called_once_with(expected_batch_file,expected_s3_key)
    final_watermark = json.loads(config["watermark_file"].read_text(encoding = "utf-8"))
    assert final_watermark["orders_bronze"] == 114

    state_data = json.loads(config["state_file"].read_text(encoding = "utf-8"))
    assert len(state_data["batches"]) == 1
    assert state_data["batches"][0]["batch_id"] == "orders_113_114"
    assert state_data["batches"][0]["first_id"]  == 113
    assert state_data["batches"][0]["last_id"] == 114
    assert state_data["batches"][0]["bronze_status"] == "completed"
    assert state_data["batches"][0]["silver_status"] == "completed"


    manifest_data = s3_manifests[config["manifest_key"]]
    assert manifest_data["silver_version"] == 1
    assert manifest_data["last_processed_order_id"] == 114
    assert manifest_data["committed_batches"] == ["orders_113_114"]
    assert len(manifest_data["committed_batches"]) == 1
