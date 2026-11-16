from src.batch_state import (
    get_pending_silver_batches,
    update_batch_status,
)
from src.silver_recovery_decision import SilverArtifactIntegrityError
from src.transform import (
    PROJECT_ROOT,
    PROCESSED_DIR,
    transform_incremental_orders,
)
from src.s3_ingestion import (
    upload_silver_orders,
    download_csv,
)
from src.silver_versioning import (
    verify_silver_artifact,
    publish_versioned_silver,
)
from src.silver_manifest import read_silver_manifest
from src.silver_recovery_decision import (
    SilverArtifactIntegrityError,
    decide_batch_recovery,
)



def recover_pending_silver_batches():
    """
    Recover pending batches using the latest committed Silver version.
    """

    # Step 1: Restore the committed Silver dataset.
    restored = restore_committed_silver()

    # Step 2: Read the current manifest.
    manifest = read_silver_manifest()

    pending_batches = get_pending_silver_batches()

    if not pending_batches:
        print("No pending Silver batches.")
        return []

    if not restored:
        print("Starting initial Silver load.")

    batches_to_publish = []
    batches_to_complete = []

    # Step 3: Examine each pending batch.
    for batch in pending_batches:

        batch_id = batch["batch_id"]

        artifact_valid = manifest is not None

        decision = decide_batch_recovery(
            batch_id,
            manifest,
            artifact_valid=artifact_valid,
        )

        # The manifest already committed this batch.
        if decision == "already_committed":

            print(
                f"Batch already committed in Silver: {batch_id}"
            )

            batches_to_complete.append(batch_id)
            continue

        # This batch still needs processing.
        batch_file = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / "incremental"
            / f"{batch_id}.csv"
        )

        s3_key = (
            f"bronze/orders/incremental/{batch_id}.csv"
        )

        batch_file = download_csv(
            s3_key,
            batch_file,
        )

        transform_incremental_orders(batch_file)

        batches_to_publish.append(batch)
        batches_to_complete.append(batch_id)

        print(
            f"Silver transformation completed: {batch_id}"
        )

    # Step 4: Publish only if there are uncommitted batches.
    if batches_to_publish:

        publish_versioned_silver(
            PROCESSED_DIR / "orders_clean.csv",
            batches_to_publish,
        )

        print("New Silver version committed.")

    # Step 5: Repair the batch ledger.
    for batch_id in batches_to_complete:

        update_batch_status(
            batch_id,
            "silver",
            "completed",
        )

        print(
            f"Silver batch completed: {batch_id}"
        )

    return batches_to_complete



def restore_committed_silver():
    """
    Restore the latest committed Silver artifact from S3.
    """

    manifest = read_silver_manifest()

    if manifest is None:
        local_file = PROCESSED_DIR / "orders_clean.csv"

        # No manifest means no committed Silver version.
        if local_file.exists():
            local_file.unlink()

        print("No committed Silver version found.")
        return False

    valid = verify_silver_artifact(manifest)

    if not valid:
        raise SilverArtifactIntegrityError(
            "Latest committed Silver artifact is missing or corrupted."
        )

    download_csv(
        manifest["file"],
        PROCESSED_DIR / "orders_clean.csv",
    )

    print(
        f"Restored committed Silver version: "
        f"{manifest['silver_version']}"
    )

    return True
