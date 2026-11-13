
import json
import pytest

from unittest.mock import patch

from src.batch_state import (
    load_batch_state,
    save_batch_state,
    register_batch,
    update_batch_status,
    get_pending_silver_batches,
)

def test_load_batch_state_when_file_missing(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        result = load_batch_state()

    assert result == {"batches": []}


def test_load_existing_batch_state(tmp_path):

    state_file = tmp_path / "batches.json"

    expected = {
        "batches": [
            {
                "batch_id": "orders_107_108",
                "first_id": 107,
                "last_id": 108,
                "bronze_status": "completed",
                "silver_status": "pending"
            }
        ]
    }

    state_file.write_text(
        json.dumps(expected),
        encoding="utf-8"
    )

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        result = load_batch_state()

    assert result == expected


def test_save_batch_state(tmp_path):

    state_file = tmp_path / "batches.json"

    state = {
        "batches": [
            {
                "batch_id": "orders_109_110",
                "first_id": 109,
                "last_id": 110,
                "bronze_status": "completed",
                "silver_status": "pending"
            }
        ]
    }

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        save_batch_state(state)

    saved = json.loads(
        state_file.read_text(encoding="utf-8")
    )

    assert saved == state


def test_invalid_batch_state_raises_error(tmp_path):

    state_file = tmp_path / "batches.json"

    state_file.write_text(
        json.dumps({"orders": 108}),
        encoding="utf-8"
    )

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        with pytest.raises(
            ValueError,
            match="Invalid batch state"
        ):
            load_batch_state()


def test_register_new_batch(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        result = register_batch(109, 110)

    assert result == {
        "batch_id": "orders_109_110",
        "first_id": 109,
        "last_id": 110,
        "bronze_status": "pending",
        "silver_status": "pending"
    }


def test_register_same_batch_twice_does_not_duplicate(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        register_batch(109, 110)
        register_batch(109, 110)

        state = load_batch_state()

    assert len(state["batches"]) == 1


def test_register_invalid_batch_range(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        with pytest.raises(ValueError):
            register_batch(110, 109)


def test_update_bronze_status(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        register_batch(109, 110)

        result = update_batch_status(
            "orders_109_110",
            "bronze",
            "completed"
        )

    assert result["bronze_status"] == "completed"
    assert result["silver_status"] == "pending"


def test_update_silver_status(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        register_batch(109, 110)

        update_batch_status(
            "orders_109_110",
            "bronze",
            "completed"
        )

        result = update_batch_status(
            "orders_109_110",
            "silver",
            "completed"
        )

    assert result["bronze_status"] == "completed"
    assert result["silver_status"] == "completed"


def test_update_unknown_batch_raises_error(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        with pytest.raises(ValueError, match="Batch not found"):
            update_batch_status(
                "orders_999_1000",
                "bronze",
                "completed"
            )


def test_invalid_stage_raises_error(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        register_batch(109, 110)

        with pytest.raises(ValueError, match="Invalid stage"):
            update_batch_status(
                "orders_109_110",
                "gold",
                "completed"
            )

def test_silver_cannot_complete_before_bronze(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        register_batch(109, 110)

        with pytest.raises(
            ValueError,
            match="Bronze must be completed"
        ):
            update_batch_status(
                "orders_109_110",
                "silver",
                "completed"
            )


def test_get_pending_silver_batches(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        # Batch 107-108: fully completed
        register_batch(107, 108)
        update_batch_status(
            "orders_107_108", "bronze", "completed"
        )
        update_batch_status(
            "orders_107_108", "silver", "completed"
        )

        # Batch 109-110: Bronze completed, Silver pending
        register_batch(109, 110)
        update_batch_status(
            "orders_109_110", "bronze", "completed"
        )

        # Batch 111-112: Bronze completed, Silver pending
        register_batch(111, 112)
        update_batch_status(
            "orders_111_112", "bronze", "completed"
        )

        result = get_pending_silver_batches()

    assert len(result) == 2

    assert [
        batch["batch_id"] for batch in result
    ] == [
        "orders_109_110",
        "orders_111_112"
    ]

def test_pending_silver_batches_excludes_bronze_pending(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        register_batch(109, 110)

        result = get_pending_silver_batches()

    assert result == []

def test_pending_silver_batches_are_sorted_by_first_id(tmp_path):

    state_file = tmp_path / "batches.json"

    with patch(
        "src.batch_state.BATCH_STATE_FILE",
        state_file
    ):
        # Register batches deliberately out of order
        register_batch(111, 112)
        register_batch(107, 108)
        register_batch(109, 110)

        for batch_id in [
            "orders_111_112",
            "orders_107_108",
            "orders_109_110"
        ]:
            update_batch_status(
                batch_id,
                "bronze",
                "completed"
            )

        result = get_pending_silver_batches()

    assert [
        batch["first_id"] for batch in result
    ] == [107, 109, 111]