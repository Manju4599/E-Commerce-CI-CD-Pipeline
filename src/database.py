
from sqlalchemy import URL, create_engine

from config.settings import (
    DB_USER,
    DB_PASSWORD,
    DB_HOST,
    DB_PORT,
    DB_NAME,
)


def create_db_engine(database=None, pool_pre_ping=False):
    """
    Create a SQLAlchemy engine using centralized database settings.

    Args:
        database: Database name. Defaults to DB_NAME.
        pool_pre_ping: Check pooled connections before using them.
    """
    database = database or DB_NAME

    if not database:
        raise ValueError("Database name is not configured")

    if not DB_USER or DB_PASSWORD is None:
        raise ValueError("Database credentials are not configured")

    connection_url = URL.create(
        drivername="mysql+pymysql",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=database,
    )

    return create_engine(
        connection_url,
        pool_pre_ping=pool_pre_ping,
    )
