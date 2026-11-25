from pathlib import Path
from src.incremental_ingestion import ingest_incremental_orders

PROJECT_ROOT = Path(__file__).resolve().parent
INTEGRATION_ROOT = PROJECT_ROOT / "data" / "integration_tests_v3"

integration_config = {
    "watermark_file": INTEGRATION_ROOT / "state" / "watermarks.json",
    "state_file": INTEGRATION_ROOT / "state" / "batches.json",
    "incremental_dir": INTEGRATION_ROOT / "raw" / "incremental",
    "processed_dir": INTEGRATION_ROOT / "processed",
    "customers_file": INTEGRATION_ROOT / "customers.csv",
    "bronze_prefix": "integration-tests-v3/bronze",
    "manifest_key": "integration-tests-v3/silver/orders/manifest.json",
    "silver_prefix": "integration-tests-v3/silver/orders",
}

if __name__ == "__main__":
    result = ingest_incremental_orders(integration_config)
    print("\nIntegration pipeline completed.")
    print("Final watermark:", result)