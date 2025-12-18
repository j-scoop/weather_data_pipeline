from __future__ import annotations

from datetime import datetime
from pathlib import Path
import logging

import yaml

from src.ingest import fetch_weather_data
from src.transform import transform_weather_data
from src.validate import validate_weather_data
from src.load import load_to_sqlite


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"

def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config(config_path: Path) -> dict:
    """Load pipeline configuration from YAML."""
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_pipeline() -> None:
    """Run the weather data ingestion and transformation pipeline."""

    setup_logging()

    config_path = PROJECT_ROOT / "config.yaml"
    config = load_config(config_path)

    logging.info(f"Config loaded: {config}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    for location in config["locations"]:
        location_name = location["name"].lower().replace(" ", "_")
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        output_filename = f"{location_name}_{timestamp}_raw.json"
        raw_output_path = RAW_DATA_DIR / output_filename

        logging.info(f"Processing location: {location['name']}")
        # Ingest
        fetch_weather_data(
            location=location,
            hourly_params=config["api"]["hourly_variables"],
            current_params=config["api"]["current_variables"],
            forecast_days=config["api"]["forecast_days"],
            raw_output_path=raw_output_path,
        )

        # Transform
        current_df, hourly_df = transform_weather_data(
            raw_json_path=raw_output_path,
            location_name=location_name,
        )

        # Validate
        validate_weather_data(
            current_df=current_df,
            hourly_df=hourly_df,
        )

        logging.info(
            f"Current rows: {len(current_df)} | "
            f"Hourly rows: {len(hourly_df)}"
        )

        # Load
        db_path = DATA_DIR / "weather.db"
        load_to_sqlite(
            current_df=current_df,
            hourly_df=hourly_df,
            db_path=db_path,
        )

    logging.info("Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
