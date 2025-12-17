from pathlib import Path

import json

from src.transform import transform_weather_data


def test_transform_weather_data_creates_expected_tables(tmp_path: Path):
    raw_json = {
        "current_weather": {
            "time": "2025-12-17T00:00",
            "temperature": 10.0,
            "windspeed": 5.0,
            "winddirection": 180,
        },
        "hourly": {
            "time": [
                "2025-12-17T00:00",
                "2025-12-17T01:00",
            ],
            "temperature_2m": [10.0, 9.5],
            "wind_speed_10m": [5.0, 4.5],
        },
    }

    raw_path = tmp_path / "raw.json"
    raw_path.write_text(json.dumps(raw_json))

    current_df, hourly_df = transform_weather_data(
        raw_json_path=raw_path,
        location_name="TestLocation",
    )

    assert len(current_df) == 1
    assert len(hourly_df) == 2
    assert "temperature" in hourly_df.columns
    assert "wind_speed" in hourly_df.columns
