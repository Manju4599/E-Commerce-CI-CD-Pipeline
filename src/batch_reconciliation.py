
from pathlib import Path

from src.batch_state import load_batch_state
from src.silver_manifest import read_silver_manifest
from src.silver_versioning import verify_silver_artifact
from src.silver_recovery_decision import SilverArtifactIntegrityError


class BatchStateIntegrityError(Exception):
    """Raised when batch ledger and Silver manifest are inconsistent."""


def reconcile_batch_state(
    state_file: Path | None = None,
    manifest_key: str = "silver/orders/manifest.json",
):
    state = load_batch_state(state_file)

    completed_batches = [
        batch
        for batch in state["batches"]
        if batch["silver_status"] == "completed"
    ]

    if not completed_batches:
        return True

    manifest = read_silver_manifest(manifest_key)

    if manifest is None:
        raise BatchStateIntegrityError(
            "Completed Silver batches exist, but the manifest is missing."
        )

    committed_batches = manifest.get("committed_batches")

    if not isinstance(committed_batches, list):
        raise BatchStateIntegrityError(
            "Silver manifest has an invalid committed_batches field."
        )

    for batch in completed_batches:
        if batch["batch_id"] not in committed_batches:
            raise BatchStateIntegrityError(
                f"Batch {batch['batch_id']} is marked completed "
                "in the ledger but is missing from the manifest."
            )

    if not verify_silver_artifact(manifest):
        raise SilverArtifactIntegrityError(
            "Committed Silver artifact is missing or corrupted."
        )

    return True