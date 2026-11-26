
from pathlib import Path

from src.incremental_ingestion import ingest_incremental_orders


PROJECT_ROOT = Path(__file__).resolve().parents[1]
V4_ROOT = PROJECT_ROOT / "data" / "integration_tests_v4"

CONFIG = {
    "watermark_file": V4_ROOT / "state" / "watermarks.json",
    "state_file": V4_ROOT / "state" / "batches.json",
    "incremental_dir": V4_ROOT / "raw" / "incremental",
    "processed_dir": V4_ROOT / "processed",
    "customers_file": PROJECT_ROOT / "data" / "raw" / "customers.csv",
    "bronze_prefix": "integration-tests-v4/bronze",
    "manifest_key": (
        "integration-tests-v4/silver/orders/manifest.json"
    ),
    "silver_prefix": "integration-tests-v4/silver/orders",
}


def main():
    # Refuse to run if the production paths are accidentally configured.
    production_state = (
        PROJECT_ROOT / "data" / "state"
    ).resolve()

    production_processed = (
        PROJECT_ROOT / "data" / "processed"
    ).resolve()

    if CONFIG["watermark_file"].parent.resolve() == production_state:
        raise RuntimeError(
            "Refusing to use production watermark state."
        )

    if CONFIG["processed_dir"].resolve() == production_processed:
        raise RuntimeError(
            "Refusing to use production Silver directory."
        )
    print("Starting isolated V4 integration run.")
    print(f"Local V4 directory: {V4_ROOT}")
    print(f"Bronze prefix: {CONFIG['bronze_prefix']}")
    print(f"Silver prefix: {CONFIG['silver_prefix']}")

    result = ingest_incremental_orders(config=CONFIG)

    print(f"V4 integration run completed. Watermark: {result}")


if __name__ == "__main__":
    main()
