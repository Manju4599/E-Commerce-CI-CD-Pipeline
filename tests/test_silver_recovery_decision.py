
import pytest

from src.silver_recovery_decision import (
    decide_batch_recovery,
    SilverArtifactIntegrityError,
)


def test_missing_manifest_needs_processing():

    result = decide_batch_recovery(
        "orders_113_115",
        None,
    )

    assert result == "needs_processing"


def test_batch_not_in_manifest_needs_processing():

    manifest = {
        "committed_batches": [
            "orders_109_109",
            "orders_110_112",
        ]
    }

    result = decide_batch_recovery(
        "orders_113_115",
        manifest,
    )

    assert result == "needs_processing"


def test_committed_batch_with_valid_artifact():

    manifest = {
        "committed_batches": [
            "orders_113_115"
        ]
    }

    result = decide_batch_recovery(
        "orders_113_115",
        manifest,
        artifact_valid=True,
    )

    assert result == "already_committed"


def test_committed_batch_with_invalid_artifact():

    manifest = {
        "committed_batches": [
            "orders_113_115"
        ]
    }

    with pytest.raises(SilverArtifactIntegrityError):

        decide_batch_recovery(
            "orders_113_115",
            manifest,
            artifact_valid=False,
        )


def test_committed_batch_requires_verification():

    manifest = {
        "committed_batches": [
            "orders_113_115"
        ]
    }

    with pytest.raises(
        ValueError,
        match="Artifact verification is required",
    ):

        decide_batch_recovery(
            "orders_113_115",
            manifest,
        )


def test_invalid_committed_batches_structure():

    manifest = {
        "committed_batches": "orders_113_115"
    }

    with pytest.raises(
        ValueError,
        match="must be a list",
    ):

        decide_batch_recovery(
            "orders_113_115",
            manifest,
        )
