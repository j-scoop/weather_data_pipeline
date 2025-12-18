from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Dict, Any

import requests


logger = logging.getLogger(__name__)

BASE_URL = "https://api.open-meteo.com/v1/forecast"
REQUEST_TIMEOUT = 10  # seconds

def fetch_weather_data(
        location: Dict[str, Any],
        hourly_params: List[str],
        current_params: List[str],
        forecast_days: int,
        raw_output_path: Path
    ) -> None:
    """
    Ingest current and forecast weather data from Open-Meteo API for a single location.

    Fetches data from the external API and persists the raw JSON response to disk
    for reproducibility and downstream processing.

    Parameters:
    - location (Dict[str, Any]): Dictionary containing 'latitude' and 'longitude'.
    - hourly_params (list): List of hourly parameters to fetch.
    - current_params (list): List of current weather parameters to fetch.
    - forecast_days (int): Number of days to forecast.
    - raw_data_path (Path): Path to save the raw JSON response.
    """

    location_name = location["name"].lower().replace(" ", "_")

    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "forecast_days": forecast_days,
        "hourly": ",".join(hourly_params),
        "current": ",".join(current_params),
    }

    logging.info(f"Fetching weather data for {location_name} from API")

    response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()

    with raw_output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    
    logging.info(
        f"Raw weather data for {location_name} saved to {raw_output_path}"
    )
