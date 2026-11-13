from pathlib import Path

import pandas as pd
from sqlalchemy import URL, create_engine, text
from dotenv import load_dotenv
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

STATE_DIR = PROJECT_ROOT / "state"
STATE_FILE = STATE_DIR / "last_order_id.txt"
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "incremental"

engine = create_engine(
    URL.create(
        "mysql+pymysql",
        username=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "3306")),
        database=os.environ["DB_NAME"],
    )
)


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
        print(f"No new orders. Watermark remains {last_id}.")
        return

    output_file = OUTPUT_DIR / "new_orders.csv"
    df.to_csv(output_file, index=False)

    new_watermark = int(df["order_id"].max())
    temp_file = STATE_FILE.with_suffix(".tmp")
    temp_file.write_text(str(new_watermark))
    temp_file.replace(STATE_FILE)

    print(f"Extracted {len(df)} new orders.")
    print(f"Saved to: {output_file}")
    print(f"Watermark updated: {last_id} -> {new_watermark}")


if __name__ == "__main__":
    try:    
        main()
    finally:
        engine.dispose()    