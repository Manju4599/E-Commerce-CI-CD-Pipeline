
from pathlib import Path

import pandas as pd
from sqlalchemy import text
import logging
from src.database import create_db_engine

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

STATE_DIR = PROJECT_ROOT / "state"
STATE_FILE = STATE_DIR / "last_order_id.txt"
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "incremental"

engine = create_db_engine()


def main():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    last_id = int(STATE_FILE.read_text().strip())

    with engine.connect() as connection:
        df = pd.read_sql_query(
            text("""
                SELECT order_id, customer_id, order_date, amount
                FROM orders
                WHERE order_id > :last_id
                ORDER BY order_id
            """),
            connection,
            params={"last_id": last_id},
        )

    if df.empty:
        logger.info("No new orders. Watermark remains %d.", last_id)
        return

    output_file = OUTPUT_DIR / "new_orders.csv"
    df.to_csv(output_file, index=False)

    new_watermark = int(df["order_id"].max())
    temp_file = STATE_FILE.with_suffix(".tmp")
    temp_file.write_text(str(new_watermark))
    temp_file.replace(STATE_FILE)

    logger.info("Extracted %d new orders.", len(df))
    logger.info("Saved to: %s", output_file)
    logger.info(
        "Watermark updated: %d -> %d",
        last_id,
        new_watermark,
    )


if __name__ == "__main__":
    from src.logging_config import configure_logging

    configure_logging()

    try:
        main()
    finally:
        engine.dispose()