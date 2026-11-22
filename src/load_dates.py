import logging
import pandas as pd
from sqlalchemy import text

from config.settings import DB_NAME, WAREHOUSE_DB_NAME
from src.database import create_db_engine

logger = logging.getLogger(__name__)


def make_engine(database):
    return create_db_engine(database)


source_engine = make_engine(DB_NAME)
warehouse_engine = make_engine(WAREHOUSE_DB_NAME)


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

    logger.info(
        "Date dimension loaded: %d distinct dates",
        len(dates),
    )


if __name__ == "__main__":
    from src.logging_config import configure_logging

    configure_logging()

    try:
        load_dates()
    finally:
        source_engine.dispose()
        warehouse_engine.dispose()