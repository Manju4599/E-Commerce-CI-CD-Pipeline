
import pandas as pd
import pytest

from src.parquet_storage import write_parquet_atomically


def test_parquet_write_and_read(tmp_path):
    df = pd.DataFrame({
        "order_id": [101, 102, 103],
        "customer_id": [1, 2, 1],
        "amount": [500.0, 750.0, 900.0],
    })

    output_path = tmp_path / "orders.parquet"

    result = write_parquet_atomically(df, output_path)

    assert result == output_path
    assert output_path.exists()

    restored = pd.read_parquet(output_path)

    pd.testing.assert_frame_equal(restored, df)


def test_parquet_writer_rejects_wrong_extension(tmp_path):
    df = pd.DataFrame({"order_id": [101]})

    with pytest.raises(ValueError, match="extension"):
        write_parquet_atomically(
            df,
            tmp_path / "orders.csv",
        )


def test_failed_parquet_write_preserves_existing_file(
    tmp_path,
    monkeypatch,
):
    output_path = tmp_path / "orders.parquet"
    output_path.write_bytes(b"existing-data")

    df = pd.DataFrame({"order_id": [101]})

    def fail_write(*args, **kwargs):
        raise RuntimeError("Simulated write failure")

    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        fail_write,
    )

    with pytest.raises(RuntimeError, match="Simulated write failure"):
        write_parquet_atomically(df, output_path)

    assert output_path.read_bytes() == b"existing-data"

    assert not list(tmp_path.glob(".orders_*.tmp"))
