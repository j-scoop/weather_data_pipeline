import requests
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any


BASE_URL = "https://api.open-meteo.com/v1/forecast"
REQUEST_TIMEOUT = 10  # seconds

def fetch_weather_data(
        location: Dict[str, Any],
        hourly_params: List[str],
        current_params: List[str],
        forecast_days: int,
        raw_data_dir: Path
    ) -> Path:
    """
    Ingest current and forecast weather data from Open-Meteo API for a single location.

    Fetches data from the external API and persists the raw JSON response to disk
    for reproducibility and downstream processing.

    Parameters:
    - location (Dict[str, Any]): Dictionary containing 'latitude' and 'longitude'.
    - hourly_params (list): List of hourly parameters to fetch.
    - forecast_hours (int): Number of hours to forecast.
    - raw_data_path (Path): Path to save the raw JSON response.

    Returns:
    - Path: Path to the saved raw JSON file.
    """

    """
    Example query_url
    https://api.open-meteo.com/v1/forecast?
    latitude=52.52
    &longitude=13.41
    &forecast_days=7
    &hourly=
        temperature_2m,
        relative_humidity_2m
        apparent_temperature,
        precipitation,
        rain,
        cloud_cover,
        wind_speed_10m       
    &current=
        temperature_2m,
        relative_humidity_2m,
        apparent_temperature,
        precipitation,
        rain,
        cloud_cover,
        wind_speed_10m,
        wind_direction_10m
    """

    location_name = location["name"].lower().replace(" ", "_")
    query_url = f"{BASE_URL}?latitude={location['latitude']}&longitude={location['longitude']}&forecast_days={forecast_days}&hourly={','.join(hourly_params)}&current={','.join(current_params)}"

    response = requests.get(query_url)
    response.raise_for_status()
    data = response.json()

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{location_name}_{timestamp}_raw.json"
    output_path = raw_data_dir / filename

    raw_data_dir.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    
    logging.info(
        f"Raw weather data for {location_name} saved to {output_path}"
    )

    return output_path

weather_data_file = fetch_weather_data(
    location={"name": "Tokyo", "latitude": 35.7, "longitude": 139.7},
    hourly_params=["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation", "rain", "cloud_cover", "wind_speed_10m", "wind_direction_10m"],
    current_params=["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation", "rain", "cloud_cover", "wind_speed_10m", "wind_direction_10m"],
    forecast_days=7,
    raw_data_dir=Path("./raw_weather_data")
)

print(f"Weather data saved to: {weather_data_file}")
