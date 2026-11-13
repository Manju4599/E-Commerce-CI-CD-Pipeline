import pandas as pd
from unittest.mock import patch

from src.extract import extract_new_orders


def test_incremental_extraction_saves_new_orders(tmp_path):
    # Simulate three new orders from MySQL
    mock_data = pd.DataFrame({
        "order_id": [104, 105, 106],
        "customer_id": [1, 2, 3],
        "amount": [500.00, 750.00, 900.00],
    })

    with (
        patch("src.extract.load_watermark", return_value=103),
        patch("src.extract.engine.connect"),
        patch("src.extract.pd.read_sql_query", return_value=mock_data) as mock_read,
        patch("src.extract.PROJECT_ROOT", tmp_path),
    ):
        df, new_watermark = extract_new_orders()

    # Verify the extracted records
    assert len(df) == 3
    assert df["order_id"].tolist() == [104, 105, 106]

    # Verify the candidate watermark
    assert new_watermark == 106

    # Verify the SQL parameter
    assert mock_read.call_args.kwargs["params"] == {"last_id": 103}

    # Verify the CSV was created correctly
    output_file = (
        tmp_path / "data" / "raw" / "incremental" / "orders_104_106.csv"
    )

    assert output_file.exists()

    saved_df = pd.read_csv(output_file)
    assert saved_df["order_id"].tolist() == [104, 105, 106]


def test_incremental_extraction_when_no_new_orders(tmp_path):
    # Simulate an empty result from MySQL
    empty_data = pd.DataFrame(columns=["order_id", "customer_id", "amount"])

    with (
        patch("src.extract.load_watermark", return_value=106),
        patch("src.extract.engine.connect"),
        patch("src.extract.pd.read_sql_query", return_value=empty_data),
        patch("src.extract.PROJECT_ROOT", tmp_path),
    ):
        df, new_watermark = extract_new_orders()

    assert df.empty
    assert new_watermark == 106

    output_file = (
        tmp_path / "data" / "raw" / "incremental" / "orders_new.csv"
    )

    assert not output_file.exists()
    

def test_multiple_batches_are_saved_separately(tmp_path):
    first_batch = pd.DataFrame({
        "order_id": [104, 105, 106],
        "customer_id": [1, 2, 3],
        "amount": [500.00, 750.00, 900.00],
    })

    second_batch = pd.DataFrame({
        "order_id": [107, 108],
        "customer_id": [4, 5],
        "amount": [600.00, 800.00],
    })

    with (
        patch(
            "src.extract.load_watermark",
            side_effect=[103, 106],
        ),
        patch("src.extract.engine.connect"),
        patch(
            "src.extract.pd.read_sql_query",
            side_effect=[first_batch, second_batch],
        ),
        patch("src.extract.PROJECT_ROOT", tmp_path),
    ):
        _, first_watermark = extract_new_orders()
        _, second_watermark = extract_new_orders()

    incremental_dir = (
        tmp_path / "data" / "raw" / "incremental"
    )

    assert first_watermark == 106
    assert second_watermark == 108

    assert (incremental_dir / "orders_104_106.csv").exists()
    assert (incremental_dir / "orders_107_108.csv").exists()