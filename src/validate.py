from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd


logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """Raised when data validation fails."""


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: Iterable[str],
    table_name: str,
) -> None:
    """
    Validate that required columns are present in the DataFrame.
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValidationError(
            f"{table_name}: missing required columns: {missing}"
        )


def validate_dataframe_not_empty(
    df: pd.DataFrame,
    table_name: str,
) -> None:
    """
    Validate that the DataFrame contains data.
    """
    if df.empty:
        raise ValidationError(f"{table_name}: DataFrame is empty")


def validate_current_weather(df: pd.DataFrame) -> None:
    """
    Validate the current weather DataFrame, checking for
    required columns and data types.
    """
    table_name = "current_weather"

    validate_dataframe_not_empty(df, table_name)

    required_columns = [
        "time",
        "temperature",
        "relative_humidity",
        "apparent_temperature",
        "precipitation",
        "rain",
        "cloud_cover",
        "wind_speed",
        "wind_direction",
        "location_name",
        "run_timestamp",
    ]
    validate_required_columns(df, required_columns, table_name)

    if len(df) != 1:
        raise ValidationError(
            f"{table_name}: expected exactly 1 row, found {len(df)}"
        )

    if not pd.api.types.is_datetime64_any_dtype(df["time"]):
        raise ValidationError(f"{table_name}: 'time' must be datetime")

    if df["temperature"].isna().any():
        raise ValidationError(f"{table_name}: temperature contains nulls")


def validate_hourly_forecast(df: pd.DataFrame) -> None:
    """
    Validate the hourly forecast DataFrame, checking for
    required columns and data types.
    """
    table_name = "hourly_forecast"

    validate_dataframe_not_empty(df, table_name)

    required_columns = [
        "time",
        "temperature",
        "relative_humidity",
        "apparent_temperature",
        "precipitation",
        "rain",
        "cloud_cover",
        "wind_speed",
        "wind_direction",
        "location_name",
        "run_timestamp",
    ]
    validate_required_columns(df, required_columns, table_name)

    if not pd.api.types.is_datetime64_any_dtype(df["time"]):
        raise ValidationError(f"{table_name}: 'time' must be datetime")

    if df["time"].isna().any():
        raise ValidationError(f"{table_name}: 'time contains nulls")

    if df["time"].duplicated().any():
        raise ValidationError(f"{table_name}: duplicate timestamps found")

    if not df["time"].is_monotonic_increasing:
        raise ValidationError(f"{table_name}: time is not sorted")


def validate_weather_data(
    current_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
) -> None:
    """Run all weather data validations."""

    logging.info("Validating transformed data")
    validate_current_weather(current_df)
    validate_hourly_forecast(hourly_df)
