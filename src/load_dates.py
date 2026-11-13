import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def make_engine(database):
    return create_engine(
        URL.create(
            "mysql+pymysql",
            username=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.environ.get("DB_HOST", "localhost"),
            port=int(os.environ.get("DB_PORT", "3306")),
            database=database,
        )
    )


source_engine = make_engine(os.environ["DB_NAME"])
warehouse_engine = make_engine("ecommerce_warehouse")


def load_dates():
    orders = pd.read_sql(
        "SELECT DISTINCT order_date FROM orders",
        source_engine,
    )

    orders["order_date"] = pd.to_datetime(orders["order_date"])
    dates = orders.drop_duplicates("order_date").copy()

    dates["date_key"] = dates["order_date"].dt.strftime("%Y%m%d").astype(int)
    dates["day_number"] = dates["order_date"].dt.day
    dates["month_number"] = dates["order_date"].dt.month
    dates["month_name"] = dates["order_date"].dt.strftime("%B")
    dates["quarter_number"] = dates["order_date"].dt.quarter
    dates["year_number"] = dates["order_date"].dt.year

    sql = text("""
        INSERT INTO dim_date (
            date_key, full_date, day_number, month_number,
            month_name, quarter_number, year_number
        )
        VALUES (
            :date_key, :full_date, :day_number, :month_number,
            :month_name, :quarter_number, :year_number
        )
        ON DUPLICATE KEY UPDATE
            month_name = VALUES(month_name),
            quarter_number = VALUES(quarter_number),
            year_number = VALUES(year_number)
    """)

    with warehouse_engine.begin() as connection:
        for row in dates.to_dict("records"):
            row["full_date"] = row.pop("order_date").date()
            connection.execute(sql, row)

    print(f"Date dimension loaded: {len(dates)} distinct dates")


if __name__ == "__main__":
    try:
        load_dates()
    finally:
        source_engine.dispose()
        warehouse_engine.dispose()