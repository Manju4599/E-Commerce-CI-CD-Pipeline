
class SilverArtifactIntegrityError(Exception):
    """Raised when a committed Silver artifact cannot be verified."""


def decide_batch_recovery(batch_id, manifest, artifact_valid=None):
    """
    Decide whether a pending batch needs Silver processing.

    Returns:
        "already_committed" - manifest contains the batch and
                              its Silver artifact is verified.
        "needs_processing"  - batch is not recorded as committed.

    Raises:
        SilverArtifactIntegrityError - manifest claims the batch
                                       was committed, but its
                                       artifact is invalid.
    """

    if not batch_id:
        raise ValueError("batch_id cannot be empty")

    if manifest is None:
        return "needs_processing"

    committed_batches = manifest.get("committed_batches")

    if not isinstance(committed_batches, list):
        raise ValueError(
            "Manifest committed_batches must be a list"
        )

    if batch_id not in committed_batches:
        return "needs_processing"

    if artifact_valid is None:
        raise ValueError(
            "Artifact verification is required for committed batches"
        )

    if artifact_valid:
        return "already_committed"

    raise SilverArtifactIntegrityError(
        f"Silver artifact verification failed for batch: {batch_id}"
    )
