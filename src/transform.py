from __future__ import annotations

import json
from pathlib import Path
from typing import Tuple

import pandas as pd


WEATHER_JSON_FILE = Path("raw_weather_data/tokyo_20251217T033728Z_raw.json")

def load_raw_json(path: Path) -> dict:
    """Load raw weather JSON from disk."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def transform_current_weather(
    raw_data: dict,
    location_name: str,
    run_timestamp: pd.Timestamp,
) -> pd.DataFrame:
    """
    Transform current weather section into a single-row DataFrame.
    """
    current = raw_data.get("current", {})

    df = pd.DataFrame([current])

    df["location_name"] = location_name
    df["run_timestamp"] = run_timestamp

    # Convert timestamp
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], utc=True)

    # Rename columns for consistency
    column_rename_map = {
        "temperature_2m": "temperature",
        "relative_humidity_2m": "relative_humidity",
        "wind_speed_10m": "wind_speed",
        "wind_direction_10m": "wind_direction",
    }

    df = df.rename(columns=column_rename_map)

    return df


def transform_hourly_forecast(
    raw_data: dict,
    location_name: str,
    run_timestamp: pd.Timestamp,
) -> pd.DataFrame:
    """
    Transform hourly forecast section into a tabular DataFrame.
    """
    hourly = raw_data.get("hourly", {})

    df = pd.DataFrame(hourly)

    df["location_name"] = location_name
    df["run_timestamp"] = run_timestamp

    # Convert timestamps
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], utc=True)

    # Rename columns for consistency
    column_rename_map = {
        "temperature_2m": "temperature",
        "relative_humidity_2m": "relative_humidity",
        "wind_speed_10m": "wind_speed",
        "wind_direction_10m": "wind_direction",
    }

    df = df.rename(columns=column_rename_map)

    return df


def transform_weather_data(
    raw_json_path: Path,
    location_name: str = "unknown",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load raw JSON and transform into current and hourly weather DataFrames.
    """
    raw_data = load_raw_json(raw_json_path)

    run_timestamp = pd.Timestamp.utcnow()

    current_df = transform_current_weather(
        raw_data=raw_data,
        location_name=location_name,
        run_timestamp=run_timestamp,
    )

    hourly_df = transform_hourly_forecast(
        raw_data=raw_data,
        location_name=location_name,
        run_timestamp=run_timestamp,
    )

    return current_df, hourly_df
