from __future__ import annotations

import sqlite3
import logging
from pathlib import Path

import pandas as pd


logger = logging.getLogger(__name__)

def load_to_sqlite(
    current_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    db_path: Path,
) -> None:
    """
    Load weather data into a SQLite database.

    Data is appended to existing tables.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    logging.info(f"Loading data into SQLite database at {db_path}")

    with sqlite3.connect(db_path) as conn:
        current_df.to_sql(
            name="current_weather",
            con=conn,
            if_exists="append",
            index=False,
        )

        hourly_df.to_sql(
            name="hourly_weather",
            con=conn,
            if_exists="append",
            index=False,
        )

        # Add indexes to location_name and time
        apply_indexes(conn)


def apply_indexes(conn: sqlite3.Connection) -> None:
    """
    Apply indexes to the database tables for performance.
    """

    logging.info("Applying indexes to database tables")

    schema_sql = """
    CREATE INDEX IF NOT EXISTS idx_current_weather_location_time
    ON current_weather (location_name, time);

    CREATE INDEX IF NOT EXISTS idx_hourly_weather_location_time
    ON hourly_weather (location_name, time);
    """
    conn.executescript(schema_sql)
