from src.extract import PROJECT_ROOT, extract_new_orders
from src.s3_ingestion import upload_csv
from src.watermark import save_watermark

from src.batch_state import (
    register_batch,
    update_batch_status,
)

from src.silver_recovery import recover_pending_silver_batches


def ingest_incremental_orders():

    # 1. Recover previously delivered Bronze batches
    # that are still pending in Silver.
    recover_pending_silver_batches()

    # 2. Extract new records using Bronze watermark
    df, candidate_watermark = extract_new_orders()

    if df.empty:
        print("No new orders to extract.")
        return candidate_watermark

    # 3. Identify the batch
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

    s3_key = f"bronze/orders/incremental/{batch_file.name}"

    # 4. Deliver and verify Bronze
    upload_csv(batch_file, s3_key)

    print(f"Bronze upload completed: {batch_id}")

    # 5. Register the batch in the ledger
    register_batch(first_id, last_id)

    # 6. Mark Bronze as completed
    update_batch_status(
        batch_id,
        "bronze",
        "completed",
    )

    # 7. Commit Bronze watermark
    save_watermark(
        "orders_bronze",
        candidate_watermark,
    )

    print(f"Bronze watermark committed: {candidate_watermark}")

    # 8. Process pending batches into Silver
    recover_pending_silver_batches()

    return candidate_watermark

