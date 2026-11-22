
import logging
from pathlib import Path

from src.extract import PROJECT_ROOT, extract_new_orders
from src.s3_ingestion import upload_csv
from src.watermark import save_watermark

from src.batch_state import (
    register_batch,
    update_batch_status,
)

from src.silver_recovery import recover_pending_silver_batches


logger = logging.getLogger(__name__)


def ingest_incremental_orders(config=None):
    """
    Run incremental ingestion.

    Without config: use existing production paths and prefixes.
    With config: use isolated integration-test paths and prefixes.
    """

    # Preserve existing production behaviour.
    if config is None:
        recover_pending_silver_batches()

        df, candidate_watermark = extract_new_orders()

        if df.empty:
            logger.info("No new orders to extract.")
            return candidate_watermark

        first_id = int(df["order_id"].min())
        last_id = int(df["order_id"].max())

        batch_id = f"orders_{first_id}_{last_id}"

        batch_file = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / "incremental"
            / f"{batch_id}.csv"
        )

        s3_key = (
            f"bronze/orders/incremental/{batch_file.name}"
        )

        upload_csv(batch_file, s3_key)

        logger.info("Bronze upload completed: %s", batch_id)

        register_batch(first_id, last_id)

        update_batch_status(
            batch_id,
            "bronze",
            "completed",
        )

        save_watermark(
            "orders_bronze",
            candidate_watermark,
        )

        logger.info(
            "Bronze watermark committed: %d",
            candidate_watermark,
        )

        recover_pending_silver_batches()

        return candidate_watermark

    # Validate integration configuration.
    required_keys = {
        "watermark_file",
        "state_file",
        "incremental_dir",
        "processed_dir",
        "customers_file",
        "bronze_prefix",
        "manifest_key",
        "silver_prefix",
    }

    missing_keys = required_keys - config.keys()

    if missing_keys:
        raise ValueError(
            f"Missing integration configuration: {missing_keys}"
        )

    watermark_file = Path(config["watermark_file"])
    state_file = Path(config["state_file"])
    incremental_dir = Path(config["incremental_dir"])
    processed_dir = Path(config["processed_dir"])
    customers_file = Path(config["customers_file"])

    bronze_prefix = config["bronze_prefix"]
    manifest_key = config["manifest_key"]
    silver_prefix = config["silver_prefix"]

    # Shared recovery configuration.
    recovery_config = {
        "state_file": state_file,
        "incremental_dir": incremental_dir,
        "processed_dir": processed_dir,
        "customers_file": customers_file,
        "bronze_prefix": bronze_prefix,
        "manifest_key": manifest_key,
        "silver_prefix": silver_prefix,
    }

    # 1. Recover previously delivered integration batches.
    logger.info("Starting integration batch recovery.")
    recover_pending_silver_batches(**recovery_config)

    # 2. Extract using the integration watermark and directory.
    df, candidate_watermark = extract_new_orders(
        watermark_file=watermark_file,
        output_dir=incremental_dir,
    )

    if df.empty:
        logger.info("No new integration orders to extract.")
        return candidate_watermark

    # 3. Identify the batch.
    first_id = int(df["order_id"].min())
    last_id = int(df["order_id"].max())

    batch_id = f"orders_{first_id}_{last_id}"

    batch_file = incremental_dir / f"{batch_id}.csv"

    s3_key = (
        f"{bronze_prefix.rstrip('/')}/{batch_file.name}"
    )

    # 4. Deliver and verify Bronze.
    upload_csv(batch_file, s3_key)

    logger.info(
        "Integration Bronze upload completed: %s",
        batch_id,
    )

    # 5. Register the integration batch.
    register_batch(
        first_id,
        last_id,
        state_file,
    )

    # 6. Mark integration Bronze as completed.
    update_batch_status(
        batch_id,
        "bronze",
        "completed",
        state_file,
    )

    # 7. Commit the integration watermark.
    save_watermark(
        "orders_bronze",
        candidate_watermark,
        watermark_file,
    )

    logger.info(
        "Integration watermark committed: %d",
        candidate_watermark,
    )

    # 8. Transform and publish integration Silver.
    recover_pending_silver_batches(**recovery_config)

    logger.info(
        "Integration ingestion completed: %s",
        batch_id,
    )

    return candidate_watermark
