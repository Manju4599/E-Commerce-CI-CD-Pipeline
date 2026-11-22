import logging
import pandas as pd
from sqlalchemy import text

from config.settings import DB_NAME, WAREHOUSE_DB_NAME
from src.database import create_db_engine

logger = logging.getLogger(__name__)


def make_engine():
    return create_db_engine(DB_NAME)


source_engine = make_engine()
warehouse_engine = create_db_engine(WAREHOUSE_DB_NAME)


def load_customers():
    df = pd.read_sql("SELECT * FROM customers", source_engine)

    with warehouse_engine.begin() as conn:
        for row in df.to_dict("records"):
            conn.execute(
                text("""
                    INSERT INTO dim_customer
                        (source_customer_id, customer_name, city, age)
                    VALUES
                        (:customer_id, :name, :city, :age)
                    ON DUPLICATE KEY UPDATE
                        customer_name = VALUES(customer_name),
                        city = VALUES(city),
                        age = VALUES(age)
                """),
                row,
            )

    logger.info(
        "Customer dimension loaded: %d source rows",
        len(df),
    )


def load_products():
    df = pd.read_sql(
        "SELECT product_id, product_name, price, stock FROM product",
        source_engine,
    )

    with warehouse_engine.begin() as conn:
        for row in df.to_dict("records"):
            conn.execute(
                text("""
                    INSERT INTO dim_product
                        (source_product_id, product_name,
                         current_price, current_stock)
                    VALUES
                        (:product_id, :product_name, :price, :stock)
                    ON DUPLICATE KEY UPDATE
                        product_name = VALUES(product_name),
                        current_price = VALUES(price),
                        current_stock = VALUES(stock)
                """),
                row,
            )

    logger.info(
        "Product dimension loaded: %d source rows",
        len(df),
    )


if __name__ == "__main__":
    from src.logging_config import configure_logging

    configure_logging()

    try:
        load_customers()
        load_products()
    finally:
        source_engine.dispose()
        warehouse_engine.dispose()  