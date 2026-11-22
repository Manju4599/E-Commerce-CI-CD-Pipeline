
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BATCH_STATE_FILE = (
    PROJECT_ROOT / "data" / "state" / "batches.json"
)


def load_batch_state(
    state_file: Path | None = None,
):
    if state_file is None:
        state_file = BATCH_STATE_FILE

    if not state_file.exists():
        return {"batches": []}

    with open(state_file, "r", encoding="utf-8") as file:
        state = json.load(file)

    if not isinstance(state, dict) or not isinstance(
        state.get("batches"), list
    ):
        raise ValueError("Invalid batch state file structure")

    return state


def save_batch_state(
    state,
    state_file: Path | None = None,
):
    if state_file is None:
        state_file = BATCH_STATE_FILE

    state_file.parent.mkdir(parents=True, exist_ok=True)

    temp_file = state_file.with_suffix(".tmp")

    try:
        with open(temp_file, "w", encoding="utf-8") as file:
            json.dump(state, file, indent=4)

        temp_file.replace(state_file)

    finally:
        if temp_file.exists():
            temp_file.unlink()


def register_batch(
    first_id,
    last_id,
    state_file: Path | None = None,
):
    if (
        not isinstance(first_id, int)
        or isinstance(first_id, bool)
        or not isinstance(last_id, int)
        or isinstance(last_id, bool)
    ):
        raise TypeError("Order IDs must be integers")

    if first_id <= 0 or last_id <= 0:
        raise ValueError("Order IDs must be positive")

    if first_id > last_id:
        raise ValueError("first_id cannot be greater than last_id")

    state = load_batch_state(state_file)

    batch_id = f"orders_{first_id}_{last_id}"

    for batch in state["batches"]:
        if batch["batch_id"] == batch_id:
            return batch

    new_batch = {
        "batch_id": batch_id,
        "first_id": first_id,
        "last_id": last_id,
        "bronze_status": "pending",
        "silver_status": "pending",
    }

    state["batches"].append(new_batch)
    save_batch_state(state, state_file)

    return new_batch


def update_batch_status(
    batch_id,
    stage,
    status,
    state_file: Path | None = None,
):
    valid_stages = {"bronze", "silver"}
    valid_statuses = {"pending", "completed"}

    if stage not in valid_stages:
        raise ValueError(f"Invalid stage: {stage}")

    if status not in valid_statuses:
        raise ValueError(f"Invalid status: {status}")

    state = load_batch_state(state_file)

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
            current_status = batch[status_key]

            if current_status == "completed" and status == "pending":
                raise ValueError(
                    f"Cannot downgrade {stage} status from "
                    "completed to pending"
                )

            if current_status == status:
                return batch

            batch[status_key] = status
            save_batch_state(state, state_file)

            return batch

    raise ValueError(f"Batch not found: {batch_id}")


def get_pending_silver_batches(
    state_file: Path | None = None,
):
    state = load_batch_state(state_file)

    pending_batches = [
        batch
        for batch in state["batches"]
        if (
            batch["bronze_status"] == "completed"
            and batch["silver_status"] == "pending"
        )
    ]

    pending_batches.sort(key=lambda batch: batch["first_id"])

    return pending_batches
