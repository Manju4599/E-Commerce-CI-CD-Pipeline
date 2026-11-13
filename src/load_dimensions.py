import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

def make_engine():
    return create_engine(
        URL.create(
            "mysql+pymysql",
            username=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.environ.get("DB_HOST", "localhost"),
            port=int(os.environ.get("DB_PORT", "3306")),
            database=os.environ["DB_NAME"],
        )
    )

source_engine = make_engine()

warehouse_engine = create_engine(
    URL.create(
        "mysql+pymysql",
        username=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "3306")),
        database="ecommerce_warehouse",
    )
)


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

    print(f"Customer dimension loaded: {len(df)} source rows")


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
                        current_price = VALUES(current_price),
                        current_stock = VALUES(current_stock)
                """),
                row,
            )

    print(f"Product dimension loaded: {len(df)} source rows")


if __name__ == "__main__":
    try:
        load_customers()
        load_products()
    finally:
        source_engine.dispose()
        warehouse_engine.dispose()
        