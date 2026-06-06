"""
Ingestion Script - Weather Data
Source: Open-Meteo API (free, no API key required)
Cities: Paris, Shanghai, London, New York
Layer: raw/weather/
"""

import requests
import json
import os
from datetime import datetime, timezone
from ingestion.date_utils import get_target_date, target_datetime_iso

# Cities configuration
CITIES = {
    "paris": {"lat": 48.8566, "lon": 2.3522, "name": "Paris"},
    "shanghai": {"lat": 31.2304, "lon": 121.4737, "name": "Shanghai"},
    "london": {"lat": 51.5074, "lon": -0.1278, "name": "London"},
    "new_york": {"lat": 40.7128, "lon": -74.0060, "name": "New York"},
}

DATA_LAKE_BASE = "/opt/airflow/data"


def fetch_current_weather(city_key: str, city_info: dict, target_date: str) -> dict:
    """Fetch current weather and forecast from Open-Meteo API."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": city_info["lat"],
        "longitude": city_info["lon"],
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "weather_code",
            "wind_speed_10m",
        ],
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code",
        ],
        "timezone": "auto",
        "forecast_days": 7,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Add metadata
    data["_metadata"] = {
        "city_key": city_key,
        "city_name": city_info["name"],
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "open-meteo.com",
        "target_date": target_date,
    }

    return data


def fetch_historical_weather(city_key: str, city_info: dict, target_date: str) -> dict:
    """Fetch historical weather and adapt the selected noon record to current-like fields."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": city_info["lat"],
        "longitude": city_info["lon"],
        "start_date": target_date,
        "end_date": target_date,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "weather_code",
            "wind_speed_10m",
        ],
        "timezone": "UTC",
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        raise RuntimeError(f"No historical hourly weather returned for {city_info['name']} on {target_date}")

    target_time = f"{target_date}T12:00"
    idx = times.index(target_time) if target_time in times else min(len(times) - 1, 12)

    def hourly_value(name):
        values = hourly.get(name) or []
        return values[idx] if idx < len(values) else None

    data["current"] = {
        "time": times[idx],
        "temperature_2m": hourly_value("temperature_2m"),
        "relative_humidity_2m": hourly_value("relative_humidity_2m"),
        "precipitation": hourly_value("precipitation"),
        "weather_code": hourly_value("weather_code"),
        "wind_speed_10m": hourly_value("wind_speed_10m"),
    }
    data["_metadata"] = {
        "city_key": city_key,
        "city_name": city_info["name"],
        "ingested_at": target_datetime_iso(target_date),
        "source": "open-meteo.com historical archive",
        "target_date": target_date,
        "selected_hour_utc": data["current"]["time"],
    }

    return data


def fetch_weather(city_key: str, city_info: dict, target_date: str) -> dict:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if target_date < today:
        return fetch_historical_weather(city_key, city_info, target_date)
    if target_date > today:
        raise ValueError("target_date cannot be in the future for this pipeline")
    return fetch_current_weather(city_key, city_info, target_date)


def save_raw(city_key: str, data: dict, date_str: str):
    """Save raw JSON to data lake following naming convention."""
    path = os.path.join(
        DATA_LAKE_BASE, "raw", "weather", date_str
    )
    os.makedirs(path, exist_ok=True)

    filename = os.path.join(path, f"{city_key}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"[Weather] Saved: {filename}")
    return filename


def run_ingestion(**kwargs):
    """Main ingestion function called by Airflow."""
    date_str = get_target_date(kwargs)
    results = []

    for city_key, city_info in CITIES.items():
        try:
            print(f"[Weather] Fetching data for {city_info['name']}...")
            data = fetch_weather(city_key, city_info, date_str)
            filepath = save_raw(city_key, data, date_str)
            results.append({"city": city_key, "status": "success", "file": filepath})
        except Exception as e:
            print(f"[Weather] ERROR for {city_key}: {e}")
            results.append({"city": city_key, "status": "error", "error": str(e)})

    print(f"[Weather] Ingestion complete: {results}")
    return results


if __name__ == "__main__":
    # Test locally
    run_ingestion()
