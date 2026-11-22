from pathlib import Path

import pandas as pd
from sqlalchemy import text

from config.settings import DB_NAME, WAREHOUSE_DB_NAME
from src.database import create_db_engine

import logging

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def make_engine(database):
    return create_db_engine(database)


source_engine = make_engine(DB_NAME)
warehouse_engine = make_engine(WAREHOUSE_DB_NAME)


def load_fact_sales():
    # 1. Extract order-line data from the source database.
    source_query = text("""
        SELECT
            oi.order_item_id AS source_order_item_id,
            o.order_id AS source_order_id,
            o.customer_id AS source_customer_id,
            oi.product_id AS source_product_id,
            o.order_date,
            oi.quantity,
            oi.unit_price
        FROM orders o
        JOIN order_items oi
            ON o.order_id = oi.order_id
        ORDER BY o.order_id, oi.order_item_id
    """)

    sales = pd.read_sql_query(source_query, source_engine)

    if sales.empty:
        logger.info("No order items found. Nothing to load.")
        return

    # 2. Retrieve the warehouse surrogate-key mappings.
    customers = pd.read_sql_query(
        text("""
            SELECT customer_key, source_customer_id
            FROM dim_customer
        """),
        warehouse_engine,
    )

    products = pd.read_sql_query(
        text("""
            SELECT product_key, source_product_id
            FROM dim_product
        """),
        warehouse_engine,
    )

    dates = pd.read_sql_query(
        text("""
            SELECT date_key, full_date
            FROM dim_date
        """),
        warehouse_engine,
    )

    # 3. Map source IDs to warehouse keys.
    sales["order_date"] = pd.to_datetime(sales["order_date"])
    dates["full_date"] = pd.to_datetime(dates["full_date"])

    sales = sales.merge(
        customers,
        on="source_customer_id",
        how="left",
        validate="many_to_one",
    )

    sales = sales.merge(
        products,
        on="source_product_id",
        how="left",
        validate="many_to_one",
    )

    sales = sales.merge(
        dates,
        left_on="order_date",
        right_on="full_date",
        how="left",
        validate="many_to_one",
    )

    required_keys = ["customer_key", "product_key", "date_key"]

    if sales[required_keys].isna().any().any():
        raise ValueError(
            "Missing warehouse dimension keys. "
            "Load or correct the dimensions before loading sales."
        )

    # 4. Prepare records for insertion.
    records = sales[
        [
            "source_order_id",
            "source_order_item_id",
            "customer_key",
            "product_key",
            "date_key",
            "quantity",
            "unit_price",
        ]
    ].to_dict("records")

    # 5. Insert or update existing source order lines.
    insert_sql = text("""
        INSERT INTO fact_sales (
            source_order_id,
            source_order_item_id,
            customer_key,
            product_key,
            date_key,
            quantity,
            unit_price
        )
        VALUES (
            :source_order_id,
            :source_order_item_id,
            :customer_key,
            :product_key,
            :date_key,
            :quantity,
            :unit_price
        )
        ON DUPLICATE KEY UPDATE
            customer_key = VALUES(customer_key),
            product_key = VALUES(product_key),
            date_key = VALUES(date_key),
            quantity = VALUES(quantity),
            unit_price = VALUES(unit_price)
    """)

    with warehouse_engine.begin() as connection:
        connection.execute(insert_sql, records)

    logger.info(
        "Fact sales load completed: %d source order lines",
        len(records),
    )

if __name__ == "__main__":
    from src.logging_config import configure_logging

    configure_logging()

    try:
        load_fact_sales()
    finally:
        source_engine.dispose()
        warehouse_engine.dispose()