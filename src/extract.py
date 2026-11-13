import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from src.watermark import load_watermark

# Locate the project root and load credentials
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

# Build the MySQL connection
connection_url = URL.create(
    drivername="mysql+pymysql",
    username=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    host=os.environ.get("DB_HOST", "localhost"),
    port=int(os.environ.get("DB_PORT", "3306")),
    database=os.environ["DB_NAME"],
)

engine = create_engine(connection_url, pool_pre_ping=True)

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

            print(
                f"Extracted {len(df)} rows from {table} "
                f"-> {output_file.name}"
            )
            

def extract_new_orders():
    # 1. Load the last successfully processed order ID
    last_id = load_watermark("orders_bronze")

    print(f"Starting incremental extraction after order_id: {last_id}")

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
        print("No new orders found.")
        return df, last_id

    # 4. Save newly extracted records separately
    output_dir = PROJECT_ROOT / "data" / "raw" / "incremental"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    first_id = int(df['order_id'].min())
    last_extracted_id = int(df['order_id'].max())
    
    output_file = output_dir / f"orders_{first_id}_{last_extracted_id}.csv"
    df.to_csv(output_file, index=False)

    # 5. Identify the highest extracted order ID
    new_watermark = last_extracted_id

    print(f"Extracted {len(df)} new orders.")
    print(f"New watermark candidate: {new_watermark}")
    print(f"Saved to: {output_file}")

    return df, new_watermark


if __name__ == "__main__":
    try:
        extract_tables()
    finally:
        engine.dispose()