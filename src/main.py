from datetime import datetime
from ingest import fetch_weather_data
from transform import transform_weather_data
from pathlib import Path
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"


def load_config(config_path: Path) -> dict:
    """Load pipeline configuration from YAML."""
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_pipeline() -> None:
    """Run the weather data ingestion and transformation pipeline."""
    config_path = PROJECT_ROOT / "config.yaml"
    config = load_config(config_path)

    print(f"Config loaded: {config}")

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    for location in config["locations"]:
        location_name = location["name"].lower().replace(" ", "_")
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        output_filename = f"{location_name}_{timestamp}_raw.json"
        raw_output_path = RAW_DATA_DIR / output_filename

        print(f"Processing location: {location['name']}")
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

        print(
            f"Current rows: {len(current_df)} | "
            f"Hourly rows: {len(hourly_df)}"
        )

    print("Pipeline run completed successfully.")


if __name__ == "__main__":
    run_pipeline()
