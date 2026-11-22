from pathlib import Path

from src.batch_reconciliation import (
    reconcile_batch_state,
    BatchStateIntegrityError,
)

state_file = Path("data/integration_tests/state/batches.json")

try:
    result = reconcile_batch_state(
        state_file=state_file,
        manifest_key="integration-tests/silver/orders/manifest.json",
    )
    print("Reconciliation result:", result)

except BatchStateIntegrityError as error:
    print("RECONCILIATION DETECTED INCONSISTENCY:", error)