"""
Ingestion Script - Air Quality Data
Source: Open-Meteo Air Quality API (free, no key required)
Cities: Paris, Shanghai, London, New York
Layer: raw/air_quality/
"""

import json
import os
from datetime import datetime, timezone

import requests

from ingestion.date_utils import get_target_dates, target_datetime_iso

CITIES = {
    "paris": {"lat": 48.8566, "lon": 2.3522, "name": "Paris"},
    "shanghai": {"lat": 31.2304, "lon": 121.4737, "name": "Shanghai"},
    "london": {"lat": 51.5074, "lon": -0.1278, "name": "London"},
    "new_york": {"lat": 40.7128, "lon": -74.0060, "name": "New York"},
}

DATA_LAKE_BASE = "/opt/airflow/data"
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
AIR_QUALITY_VARIABLES = [
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "dust",
    "uv_index",
    "us_aqi",
    "european_aqi",
]


def select_hourly_record(data: dict, target_date: str) -> dict:
    """Select the noon UTC record from Open-Meteo hourly air quality data."""
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        raise RuntimeError(f"No hourly air quality data returned for {target_date}")

    target_time = f"{target_date}T12:00"
    idx = times.index(target_time) if target_time in times else min(len(times) - 1, 12)

    record = {"time": times[idx]}
    for variable in AIR_QUALITY_VARIABLES:
        values = hourly.get(variable) or []
        record[variable] = values[idx] if idx < len(values) else None
    return record


def fetch_air_quality(city_key: str, city_info: dict, target_date: str) -> dict:
    """Fetch hourly air quality data for one city and one target date."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if target_date > today:
        raise ValueError("target_date cannot be in the future for this pipeline")

    params = {
        "latitude": city_info["lat"],
        "longitude": city_info["lon"],
        "hourly": AIR_QUALITY_VARIABLES,
        "timezone": "UTC",
        "start_date": target_date,
        "end_date": target_date,
    }

    response = requests.get(AIR_QUALITY_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    data["current_air_quality"] = select_hourly_record(data, target_date)
    data["_metadata"] = {
        "city_key": city_key,
        "city_name": city_info["name"],
        "ingested_at": (
            target_datetime_iso(target_date)
            if target_date < today
            else datetime.now(timezone.utc).isoformat()
        ),
        "source": "Open-Meteo Air Quality API",
        "source_url": AIR_QUALITY_URL,
        "target_date": target_date,
        "selected_hour_utc": data["current_air_quality"]["time"],
        "variables": AIR_QUALITY_VARIABLES,
    }
    return data


def save_raw(city_key: str, data: dict, date_str: str):
    path = os.path.join(DATA_LAKE_BASE, "raw", "air_quality", date_str)
    os.makedirs(path, exist_ok=True)

    filename = os.path.join(path, f"{city_key}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"[AirQuality] Saved: {filename}")
    return filename


def run_ingestion(**kwargs):
    target_dates = get_target_dates(kwargs)
    results = []

    for date_str in target_dates:
        print(f"[AirQuality] Processing target date: {date_str}")
        for city_key, city_info in CITIES.items():
            try:
                print(f"[AirQuality] Fetching data for {city_info['name']} on {date_str}...")
                data = fetch_air_quality(city_key, city_info, date_str)
                filepath = save_raw(city_key, data, date_str)
                results.append({
                    "date": date_str,
                    "city": city_key,
                    "status": "success",
                    "file": filepath,
                })
            except Exception as e:
                print(f"[AirQuality] ERROR for {city_key} on {date_str}: {e}")
                results.append({
                    "date": date_str,
                    "city": city_key,
                    "status": "error",
                    "error": str(e),
                })

    failed = [r for r in results if r["status"] != "success"]
    if failed:
        raise RuntimeError(f"Air quality ingestion failed for {len(failed)} city/date item(s): {failed}")

    print(f"[AirQuality] Ingestion complete: {results}")
    return results


if __name__ == "__main__":
    run_ingestion()
