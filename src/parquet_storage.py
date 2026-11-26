
import logging
import tempfile
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def write_parquet_atomically(df: pd.DataFrame, output_path: Path):
    """
    Write a DataFrame to Parquet atomically.

    The destination is replaced only after the temporary
    Parquet file has been written successfully.
    """

    output_path = Path(output_path)

    if output_path.suffix.lower() != ".parquet":
        raise ValueError("Output file must have a .parquet extension")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(
            dir=output_path.parent,
            prefix=f".{output_path.stem}_",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)

        df.to_parquet(
            temp_path,
            engine="pyarrow",
            index=False,
        )

        # Verify the temporary file can be read.
        verified_df = pd.read_parquet(
            temp_path,
            engine="pyarrow",
        )

        if len(verified_df) != len(df):
            raise RuntimeError("Parquet row-count verification failed")

        temp_path.replace(output_path)

        logger.info(
            "Parquet file written successfully: %s (%d rows)",
            output_path,
            len(df),
        )

        return output_path

    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()
