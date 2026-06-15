"""ClickHouse storage layer for STOXX index data."""

from __future__ import annotations

import os

import psycopg
from dotenv import load_dotenv


def get_connection() -> psycopg.Connection:
    """Create a ClickHouse connection via Postgres wire protocol."""
    load_dotenv()
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=int(os.environ["PGPORT"]),
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        dbname=os.environ["PGDATABASE"],
        sslmode="require",
    )
