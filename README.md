# Weather Data Pipeline

A lightweight, self-contained Python data pipeline that ingests weather data from a public API, validates and transforms it into an analytics-ready format, and stores the results locally in SQLite.

This project is designed to demonstrate practical data engineering skills in a way that is easy to run, inspect, and extend — no cloud services or credentials required.

---

## What this project demonstrates

* API data ingestion (Open-Meteo)
* Schema validation and basic data quality checks
* Data transformation using pandas
* Append-only data modelling with run timestamps
* SQLite persistence with indexes for query performance
* Simple automated tests with pytest
* Clean, modular Python project structure

---

## Project structure

```
weather-data-pipeline/
├── src/
│   ├── __init__.py
│   ├── ingest.py
│   ├── load.py
│   ├── main.py
│   ├── transform.py
│   └── validate.py
├── tests/
│   └── test_transform.py
├── config.yaml
├── requirements.txt
├── README.md
└── .gitignore
```

---

## Requirements

* Python 3.10+
* Internet connection (for API access)

No databases, cloud accounts, or API keys are required.

---

## Setup

Clone the repository and create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Running the pipeline

From the project root:

```bash
python -m src.main
```

This will:

1. Fetch current and hourly weather data from the Open-Meteo API
2. Validate the raw response structure
3. Transform the data into tabular form
4. Store the results in a local SQLite database
5. Create indexes to optimise common query patterns

The SQLite database will be created locally (by default: `weather.db`).

---

## Running tests

Tests are written using `pytest`.

From the project root:

```bash
python -m pytest
```

The tests validate key transformation logic and ensure time columns are correctly parsed as datetimes.

---

## Data model (high level)

Two tables are created:

### `current_weather`

* `time`
* `interval`
* `temperature`
* `relative_humidity`
* `apparent_temperature`
* `precipitation`
* `rain`
* `cloud_cover`
* `wind_speed`
* `wind_direction`
* `location_name`
* `run_timestamp`

### `hourly_weather`

* `time`
* `temperature`
* `relative_humidity`
* `apparent_temperature`
* `precipitation`
* `rain`
* `cloud_cover`
* `wind_speed`
* `wind_direction`
* `location_name`
* `run_timestamp`

Indexes are created on `(location_name, time)` for both tables.

---

## Design decisions

* **SQLite** was chosen to keep the project fully local and easy to run without external services.
* Data is stored in an **append-only** manner, with a `run_timestamp` column to preserve historical runs.
* Time values are kept as explicit columns (not indexes) to simplify exports and downstream usage.
* The project avoids orchestration frameworks and cloud tooling to keep the focus on core data engineering logic.

---

## Possible extensions

* Historical weather ingestion
* CSV exports for downstream tools
* Deduplication or upsert logic
* Scheduling (cron, systemd, or similar)
* Additional validation rules

---

## Notes

This repository is intended as a small but realistic example of how I approach data ingestion and transformation problems in Python. The structure and trade-offs mirror patterns commonly used in production systems, scaled down to run locally.
