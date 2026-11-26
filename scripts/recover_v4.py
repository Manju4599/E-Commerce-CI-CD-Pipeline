from pathlib import Path

from src.silver_recovery import recover_pending_silver_batches

ROOT = Path(__file__).resolve().parents[1]
V4 = ROOT / "data" / "integration_tests_v4"

def main():

    recover_pending_silver_batches(
        state_file=V4 / "state" / "batches.json",
        incremental_dir=V4 / "raw" / "incremental",
        processed_dir=V4 / "processed",
        customers_file=ROOT / "data" / "raw" / "customers.csv",
        bronze_prefix="integration-tests-v4/bronze",
        manifest_key="integration-tests-v4/silver/orders/manifest.json",
        silver_prefix="integration-tests-v4/silver/orders",
    )
    print("V4 recovery completed.")

if "__name__" == "__main__":
    main()