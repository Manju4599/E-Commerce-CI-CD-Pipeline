
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BATCH_STATE_FILE = (
    PROJECT_ROOT / "data" / "state" / "batches.json"
)


def load_batch_state():
    """
    Load batch processing state from JSON.
    Return an empty batch structure if the file does not exist.
    """

    if not BATCH_STATE_FILE.exists():
        return {"batches": []}

    with open(BATCH_STATE_FILE, "r", encoding="utf-8") as file:
        state = json.load(file)

    if not isinstance(state, dict) or not isinstance(
        state.get("batches"), list
    ):
        raise ValueError("Invalid batch state file structure")

    return state


def save_batch_state(state):
    """
    Save batch processing state safely using atomic replacement.
    """

    BATCH_STATE_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temp_file = BATCH_STATE_FILE.with_suffix(".tmp")

    try:
        with open(temp_file, "w", encoding="utf-8") as file:
            json.dump(state, file, indent=4)

        temp_file.replace(BATCH_STATE_FILE)

    finally:
        if temp_file.exists():
            temp_file.unlink()


def register_batch(first_id, last_id):
    """
    Register an incremental batch in the batch ledger.

    A batch is uniquely identified by its first and last order IDs.
    """

    if first_id > last_id:
        raise ValueError("first_id cannot be greater than last_id")

    state = load_batch_state()

    batch_id = f"orders_{first_id}_{last_id}"

    # Avoid registering the same batch twice
    for batch in state["batches"]:
        if batch["batch_id"] == batch_id:
            return batch

    new_batch = {
        "batch_id": batch_id,
        "first_id": first_id,
        "last_id": last_id,
        "bronze_status": "pending",
        "silver_status": "pending"
    }

    state["batches"].append(new_batch)

    save_batch_state(state)

    return new_batch


def update_batch_status(batch_id, stage, status):
    """
    Update the processing status of a registered batch.
    """

    valid_stages = {"bronze", "silver"}
    valid_statuses = {"pending", "completed"}

    if stage not in valid_stages:
        raise ValueError(f"Invalid stage: {stage}")

    if status not in valid_statuses:
        raise ValueError(f"Invalid status: {status}")
    

    state = load_batch_state()

    for batch in state["batches"]:

        if batch["batch_id"] == batch_id:
            
            if (
                stage == "silver"
                and status == "completed"
                and batch["bronze_status"] != "completed"
                ):
                    raise ValueError(
                        "Bronze must be completed before Silver"
                    )

            status_key = f"{stage}_status"

            batch[status_key] = status

            save_batch_state(state)

            return batch
        

    raise ValueError(f"Batch not found: {batch_id}")


def get_pending_silver_batches():
    """
    Return batches successfully delivered to Bronze
    but not yet completed in Silver.
    """

    state = load_batch_state()

    pending_batches = [
        batch
        for batch in state["batches"]
        if (
            batch["bronze_status"] == "completed"
            and batch["silver_status"] == "pending"
        )
    ]

    # Process older batches first
    pending_batches.sort(
        key=lambda batch: batch["first_id"]
    )

    return pending_batches