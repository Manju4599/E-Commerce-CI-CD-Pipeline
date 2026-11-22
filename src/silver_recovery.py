
import logging
from pathlib import Path

from src.batch_state import (
    get_pending_silver_batches,
    update_batch_status,
)

from src.transform import (
    PROJECT_ROOT,
    PROCESSED_DIR,
    RAW_DIR,
    transform_incremental_orders,
)

from src.s3_ingestion import download_csv

from src.silver_versioning import (
    verify_silver_artifact,
    publish_versioned_silver,
)

from src.silver_manifest import (
    MANIFEST_KEY,
    read_silver_manifest,
)

from src.silver_recovery_decision import (
    SilverArtifactIntegrityError,
    decide_batch_recovery,
)

from src.batch_reconciliation import reconcile_batch_state


logger = logging.getLogger(__name__)


def recover_pending_silver_batches(
    state_file=None,
    incremental_dir=None,
    processed_dir=None,
    customers_file=None,
    bronze_prefix="bronze/orders/incremental",
    manifest_key=None,
    silver_prefix="silver/orders",
):
    """
    Recover pending Bronze batches and publish them to Silver.

    Optional paths and prefixes allow isolated integration testing.
    Existing no-argument production calls remain supported.
    """

    custom_config = any([
        state_file is not None,
        incremental_dir is not None,
        processed_dir is not None,
        customers_file is not None,
        manifest_key is not None,
        bronze_prefix != "bronze/orders/incremental",
        silver_prefix != "silver/orders",
    ])

    if incremental_dir is None:
        incremental_dir = (
            PROJECT_ROOT / "data" / "raw" / "incremental"
        )

    if processed_dir is None:
        processed_dir = (
            PROJECT_ROOT / "data" / "processed"
        )

    if manifest_key is None:
        manifest_key = MANIFEST_KEY
    
    reconcile_batch_state(
        state_file=state_file,
        manifest_key=manifest_key,
    )
    
    if state_file is not None:
        pending_batches = get_pending_silver_batches(state_file)
    else:
        pending_batches = get_pending_silver_batches()
        


    # Step 1: Restore the committed Silver dataset.
    if custom_config:
        restored = restore_committed_silver(
            processed_dir=processed_dir,
            manifest_key=manifest_key,
        )
    else:
        restored = restore_committed_silver()

    # Step 2: Read the current manifest.
    if custom_config:
        manifest = read_silver_manifest(manifest_key)
    else:
        manifest = read_silver_manifest()

    # Step 3: Retrieve pending batches.
    pending_batches = get_pending_silver_batches(state_file=None)

    if not pending_batches:
        logger.info("No pending Silver batches.")
        return []

    if not restored:
        logger.info("Starting initial Silver load.")

    logger.info(
        "Found %d pending Silver batches.",
        len(pending_batches),
    )

    batches_to_publish = []
    batches_to_complete = []

    # Step 4: Examine each pending batch.
    for batch in pending_batches:

        batch_id = batch["batch_id"]

        # restore_committed_silver has already verified
        # the committed artifact when a manifest exists.
        artifact_valid = manifest is not None

        decision = decide_batch_recovery(
            batch_id,
            manifest,
            artifact_valid=artifact_valid,
        )

        logger.info(
            "Recovery decision for batch %s: %s",
            batch_id,
            decision,
        )

        if decision == "already_committed":
            logger.info(
                "Batch already committed in Silver: %s",
                batch_id,
            )

            batches_to_complete.append(batch_id)
            continue

        batch_file = (
            Path(incremental_dir) / f"{batch_id}.csv"
        )

        s3_key = (
            f"{bronze_prefix.rstrip('/')}/{batch_id}.csv"
        )

        logger.info(
            "Recovering Bronze batch from s3://%s",
            s3_key,
        )

        batch_file = download_csv(
            s3_key,
            batch_file,
        )

        if custom_config:
            transform_incremental_orders(
                batch_file,
                processed_dir=processed_dir,
                customers_file=customers_file,
            )
        else:
            transform_incremental_orders(batch_file)

        batches_to_publish.append(batch)
        batches_to_complete.append(batch_id)

        logger.info(
            "Silver transformation completed: %s",
            batch_id,
        )

    # Step 5: Publish the consolidated Silver dataset.
    if batches_to_publish:

        silver_file = Path(processed_dir) / "orders_clean.csv"

        if custom_config:
            publish_versioned_silver(
                silver_file,
                batches_to_publish,
                manifest_key=manifest_key,
                silver_prefix=silver_prefix,
            )
        else:
            publish_versioned_silver(
                silver_file,
                batches_to_publish,
            )

        logger.info("New Silver version committed.")

    # Step 6: Repair/update the batch ledger.
    for batch_id in batches_to_complete:

        if state_file is not None:
            update_batch_status(
                batch_id,
                "silver",
                "completed",
                state_file,
            )
        else:
            update_batch_status(
                batch_id,
                "silver",
                "completed",
            )

            logger.info(
                "Silver batch completed: %s",
                batch_id,
            )

    return batches_to_complete


def restore_committed_silver(
    processed_dir=None,
    manifest_key=None,
):
    """
    Restore the latest committed Silver artifact from S3.

    Only the specified local directory is modified.
    """

    custom_config = (
        processed_dir is not None
        or manifest_key is not None
    )

    if processed_dir is None:
        processed_dir = (
            PROJECT_ROOT / "data" / "processed"
        )

    if manifest_key is None:
        manifest_key = MANIFEST_KEY

    processed_dir = Path(processed_dir)
    local_file = processed_dir / "orders_clean.csv"

    if custom_config:
        manifest = read_silver_manifest(manifest_key)
    else:
        manifest = read_silver_manifest()

    if manifest is None:

        if local_file.exists():
            local_file.unlink()

        logger.info("No committed Silver version found.")
        return False

    valid = verify_silver_artifact(manifest)

    if not valid:
        raise SilverArtifactIntegrityError(
            "Latest committed Silver artifact is missing or corrupted."
        )

    download_csv(
        manifest["file"],
        local_file,
    )

    logger.info(
        "Restored committed Silver version: %s",
        manifest["silver_version"],
    )

    return True
