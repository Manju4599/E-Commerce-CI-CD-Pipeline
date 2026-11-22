
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.database import create_db_engine
from src.watermark import load_watermark


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

engine = create_db_engine(pool_pre_ping=True)

TABLES = ["customers", "orders", "product", "order_items"]


def extract_tables():
    output_dir = PROJECT_ROOT / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)

    with engine.connect() as connection:
        for table in TABLES:
            # Table names come from our fixed allowlist above.
            query = text(f"SELECT * FROM `{table}`")
            df = pd.read_sql_query(query, connection)

            output_file = output_dir / f"{table}.csv"
            df.to_csv(output_file, index=False)

            logger.info(
                "Extracted %d rows from %s -> %s",
                len(df),
                table,
                output_file.name,
            )


def extract_new_orders(
    watermark_file: Path | None = None,
    output_dir: Path | None = None,
):
    # 1. Load the last successfully processed order ID
    last_id = load_watermark(
        "orders_bronze",
        watermark_file,
    )

    logger.info(
        "Starting incremental extraction after order_id: %d",
        last_id,
    )

    # 2. Query only records newer than the watermark
    query = text("""
        SELECT *
        FROM orders
        WHERE order_id > :last_id
        ORDER BY order_id
    """)

    with engine.connect() as connection:
        df = pd.read_sql_query(
            query,
            connection,
            params={"last_id": last_id},
        )

    # 3. Handle the case where no new records exist
    if df.empty:
        logger.info("No new orders found.")
        return df, last_id

    # 4. Save newly extracted records separately
    if output_dir is None:
        output_dir = PROJECT_ROOT / "data" / "raw" / "incremental"

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    first_id = int(df["order_id"].min())
    last_extracted_id = int(df["order_id"].max())

    output_file = (
        output_dir
        / f"orders_{first_id}_{last_extracted_id}.csv"
    )

    df.to_csv(output_file, index=False)

    # 5. Identify the highest extracted order ID
    new_watermark = last_extracted_id

    logger.info("Extracted %d new orders.", len(df))
    logger.info("New watermark candidate: %d", new_watermark)
    logger.info("Saved to: %s", output_file)

    return df, new_watermark


if __name__ == "__main__":
    try:
        from src.logging_config import configure_logging
        configure_logging()
        extract_tables()
    finally:
        engine.dispose()
