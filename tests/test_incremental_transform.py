
import pandas as pd
import pytest

from unittest.mock import patch

from src.transform import load_validated_incremental_batch


def test_load_validated_incremental_batch(tmp_path):

    batch_file = tmp_path / "orders_107_108.csv"

    batch = pd.DataFrame({
        "order_id": [107, 108],
        "customer_id": [1, 2],
        "order_date": ["2026-11-09", "2026-11-10"],
        "amount": [500.00, 750.00]
    })

    batch.to_csv(batch_file, index=False)

    customers = pd.DataFrame({
        "customer_id": [1, 2, 3]
    })

    with patch(
        "src.transform.RAW_DIR",
        tmp_path
    ):
        customers.to_csv(
            tmp_path / "customers.csv",
            index=False
        )

        result = load_validated_incremental_batch(batch_file)

    assert len(result) == 2
    assert result["order_id"].tolist() == [107, 108]


def test_missing_incremental_batch():

    with pytest.raises(FileNotFoundError):
        load_validated_incremental_batch(
            "missing_orders.csv"
        )


def test_invalid_customer_reference(tmp_path):

    batch_file = tmp_path / "orders_109.csv"

    batch = pd.DataFrame({
        "order_id": [109],
        "customer_id": [999],
        "order_date": ["2026-11-10"],
        "amount": [900.00]
    })

    batch.to_csv(batch_file, index=False)

    customers = pd.DataFrame({
        "customer_id": [1, 2, 3]
    })

    with patch(
        "src.transform.RAW_DIR",
        tmp_path
    ):
        customers.to_csv(
            tmp_path / "customers.csv",
            index=False
        )

        with pytest.raises(
            ValueError,
            match="nonexistent customers"
        ):
            load_validated_incremental_batch(batch_file)



from src.transform import merge_incremental_orders


def test_merge_adds_new_orders(tmp_path):

    existing = pd.DataFrame({
        "order_id": [101, 102],
        "customer_id": [1, 2],
        "order_date": ["2026-11-01", "2026-11-02"],
        "amount": [500.00, 750.00]
    })

    new = pd.DataFrame({
        "order_id": [103, 104],
        "customer_id": [3, 4],
        "order_date": ["2026-11-03", "2026-11-04"],
        "amount": [900.00, 600.00]
    })

    with patch("src.transform.PROCESSED_DIR", tmp_path):

        existing.to_csv(
            tmp_path / "orders_clean.csv",
            index=False
        )

        result = merge_incremental_orders(new)

    assert len(result) == 4
    assert result["order_id"].tolist() == [101, 102, 103, 104]


def test_reprocessing_same_batch_does_not_duplicate(tmp_path):

    existing = pd.DataFrame({
        "order_id": [101, 102],
        "customer_id": [1, 2],
        "order_date": ["2026-11-01", "2026-11-02"],
        "amount": [500.00, 750.00]
    })

    new = pd.DataFrame({
        "order_id": [102, 103],
        "customer_id": [2, 3],
        "order_date": ["2026-11-02", "2026-11-03"],
        "amount": [750.00, 900.00]
    })

    with patch("src.transform.PROCESSED_DIR", tmp_path):

        existing.to_csv(
            tmp_path / "orders_clean.csv",
            index=False
        )

        result = merge_incremental_orders(new)

    assert len(result) == 3
    assert result["order_id"].tolist() == [101, 102, 103]
    assert result["order_id"].is_unique


def test_conflicting_existing_order_raises_error(tmp_path):

    existing = pd.DataFrame({
        "order_id": [101],
        "customer_id": [1],
        "order_date": ["2026-11-01"],
        "amount": [500.00]
    })

    new = pd.DataFrame({
        "order_id": [101],
        "customer_id": [1],
        "order_date": ["2026-11-01"],
        "amount": [999.00]
    })

    with patch("src.transform.PROCESSED_DIR", tmp_path):

        existing.to_csv(
            tmp_path / "orders_clean.csv",
            index=False
        )

        with pytest.raises(
            ValueError,
            match="Conflicting data"
        ):
            merge_incremental_orders(new)


def test_transform_incremental_orders_end_to_end(tmp_path):

    from src.transform import transform_incremental_orders

    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"

    raw_dir.mkdir()
    processed_dir.mkdir()

    # Customer reference data
    customers = pd.DataFrame({
        "customer_id": [1, 2, 3]
    })

    customers.to_csv(
        raw_dir / "customers.csv",
        index=False
    )

    # Existing Silver records
    existing = pd.DataFrame({
        "order_id": [101, 102],
        "customer_id": [1, 2],
        "order_date": ["2026-11-01", "2026-11-02"],
        "amount": [500.00, 750.00]
    })

    existing.to_csv(
        processed_dir / "orders_clean.csv",
        index=False
    )

    # New Bronze batch
    new_batch = pd.DataFrame({
        "order_id": [103, 104],
        "customer_id": [2, 3],
        "order_date": ["2026-11-03", "2026-11-04"],
        "amount": [900.00, 600.00]
    })

    batch_file = raw_dir / "orders_103_104.csv"
    new_batch.to_csv(batch_file, index=False)

    with (
        patch("src.transform.RAW_DIR", raw_dir),
        patch("src.transform.PROCESSED_DIR", processed_dir)
    ):
        result = transform_incremental_orders(batch_file)

    # Verify the returned DataFrame
    assert len(result) == 4
    assert result["order_id"].tolist() == [
        101, 102, 103, 104
    ]

    # Verify the actual persisted Silver CSV
    saved = pd.read_csv(
        processed_dir / "orders_clean.csv"
    )

    assert len(saved) == 4
    assert saved["order_id"].is_unique
