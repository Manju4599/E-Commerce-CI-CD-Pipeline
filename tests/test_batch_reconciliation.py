import json
import pytest

from src.batch_reconciliation import (
    reconcile_batch_state,
    BatchStateIntegrityError,
)
from src.silver_recovery_decision import SilverArtifactIntegrityError

def test_completed_batch_with_missing_manifest_raises_error(tmp_path, monkeypatch):
    state_file = tmp_path / "batches.json"

    state = {
        "batches": [
            {
                "batch_id": "orders_110_110",
                "first_id": 110,
                "last_id": 110,
                "bronze_status": "completed",
                "silver_status": "completed",
            }
        ]
    }

    state_file.write_text(json.dumps(state), encoding="utf-8")

    monkeypatch.setattr(
        "src.batch_reconciliation.read_silver_manifest",
        lambda manifest_key: None,
    )

    with pytest.raises(BatchStateIntegrityError):
        reconcile_batch_state(
            state_file=state_file,
            manifest_key="integration-tests/silver/orders/manifest.json",
        )
        
def test_completed_batch_absent_from_manifest(tmp_path, monkeypatch):
    state_file = tmp_path/ "batches.json"
    
    state = {
        "batches": [
            {
                "batch_id": "orders_110_110",
                "first_id": 110,
                "last_id": 110,
                "bronze_status": "completed",
                "silver_status": "completed",
            }
        ]
    }
    
    state_file.write_text(json.dumps(state), encoding="utf-8")
        
    fake_manifest = {
        "silver_version": 1,
        "file": "integration-tests/silver/orders/orders_v001.csv",
        "sha256": "some_sha256_value",
        "last_processed_order_id": 109,
        "committed_batches": ["orders_100_109"],
    }
    
    monkeypatch.setattr("src.batch_reconciliation.read_silver_manifest", 
                        lambda manifest_key: fake_manifest,)
    
    with pytest.raises(BatchStateIntegrityError):
        reconcile_batch_state(
            state_file=state_file,
            manifest_key="integration-tests/silver/orders/manifest.json",
            
        )
        
def test_completed_batch_with_valid_artifact(tmp_path, monkeypatch):
    state_file = tmp_path / "batches.json"
    
    state = {
            "batches": [
                {
                    "batch_id": "orders_110_110",
                    "first_id": 110,
                    "last_id": 110,
                    "bronze_status": "completed",
                    "silver_status": "completed",
                }
            ]
        }
    
    state_file.write_text(json.dumps(state),encoding="utf-8")
    
    fake_manifest = {
            "silver_version": 1,
            "file": "integration-tests/silver/orders/orders_v001.csv",
            "sha256": "some_sha256_value",
            "last_processed_order_id": 110,
            "committed_batches": ["orders_110_110"],
        }
    
    monkeypatch.setattr("src.batch_reconciliation.read_silver_manifest",
                        lambda manifest_key: fake_manifest)
    monkeypatch.setattr("src.batch_reconciliation.verify_silver_artifact",
                        lambda manifest: True,)
    
    result = reconcile_batch_state(
        state_file = state_file,
        manifest_key = "integration-tests/silver/orders/manifest.json",
    )
    
    updated_state = json.loads(state_file.read_text(encoding="utf-8"))
    
    assert result is True
    assert updated_state == state
    
def test_completed_batch_but_corrupt_artifact(tmp_path, monkeypatch):
    state_file = tmp_path / "batches.json"
    
    state = {
        
        "batches": [
            {
                "batch_id": "orders_110_110",
                "first_id": 110,
                "last_id": 110,
                "bronze_status": "completed",
                "silver_status": "completed",
            }
        ]
    }
    
    fake_manifest = {
        "silver_version": 1,
        "file": "integration-tests/silver/orders/orders_v001.csv",
        "sha256": "some_sha256_value",
        "last_processed_order_id": 110,
        "committed_batches": ["orders_110_110"],
    }
    
    state_file.write_text(json.dumps(state),encoding="utf-8")
    
    monkeypatch.setattr("src.batch_reconciliation.verify_silver_artifact", lambda manifest_key: False,)
    monkeypatch.setattr("src.batch_reconciliation.read_silver_manifest", lambda manifest_key: fake_manifest,)
    
    with pytest.raises(SilverArtifactIntegrityError):
        reconcile_batch_state(
            state_file = state_file,
            manifest_key = "integration-tests/silver/orders/manifest.json"
        )
    
def test_pending_batch_skips_manifest_and_artifact_verify(tmp_path, monkeypatch):
    state_file = tmp_path / "batches.json"
    
    state = {
        "batches":[
            {
                "batch_id": "orders_110_110",
                "first_id": 110,
                "last_id": 110,
                "bronze_status": "completed",
                "silver_status": "pending",
            }
        ]
    }
    
    state_file.write_text(json.dumps(state), encoding="utf-8")
    
    monkeypatch.setattr("src.batch_reconciliation.read_silver_manifest", lambda manifest_key : None)
    
    def fail_if_called(*args, **kwargs):
        raise AssertionError("This function not to be called!")
    
    monkeypatch.setattr("src.batch_reconciliation.verify_silver_artifact",fail_if_called,)
    
    result = reconcile_batch_state(state_file = state_file, manifest_key = "integration-tests/silver/orders/manifest.json")
    
    assert result is True
    updated_state = json.loads(state_file.read_text(encoding = "utf-8"))
    
    assert updated_state == state