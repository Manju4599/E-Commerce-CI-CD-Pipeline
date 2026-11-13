from src.batch_state import (
    get_pending_silver_batches,
    update_batch_status,
)

from src.transform import (
    PROJECT_ROOT,
    transform_incremental_orders,
)

from src.s3_ingestion import (
    upload_silver_orders,
    download_csv,
)
def recover_pending_silver_batches():

    pending_batches = get_pending_silver_batches()

    if not pending_batches:
        print("No pending Silver batches.")
        return []

    processed_batch_ids = []

    for batch in pending_batches:

        batch_id = batch["batch_id"]

        # Local Bronze batch path
        batch_file = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / "incremental"
            / f"{batch_id}.csv"
        )

        # Corresponding Bronze S3 object
        s3_key = (
            f"bronze/orders/incremental/{batch_id}.csv"
        )

        # Download from S3 if the local file is missing
        batch_file = download_csv(
            s3_key,
            batch_file,
        )

        # Transform the recovered Bronze batch
        transform_incremental_orders(batch_file)

        processed_batch_ids.append(batch_id)

        print(
            f"Silver transformation completed: {batch_id}"
        )

    # Upload consolidated Silver data once
    upload_silver_orders()

    print("Consolidated Silver upload completed.")

    # Mark batches complete only after successful upload
    for batch_id in processed_batch_ids:

        update_batch_status(
            batch_id,
            "silver",
            "completed",
        )

        print(f"Silver batch completed: {batch_id}")

    return processed_batch_ids