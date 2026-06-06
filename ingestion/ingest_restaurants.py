"""
Ingestion Script - Restaurant Data
Source: Overpass API (OpenStreetMap) - free, no key required
Cities: Paris, Shanghai, London, New York
"""

import requests
import json
import os
import time
from datetime import datetime, timezone
from ingestion.date_utils import get_target_date, target_datetime_iso, target_overpass_datetime

CITIES = {
    "paris":    {"name": "Paris",    "bbox": (48.815, 2.224, 48.902, 2.470)},
    "shanghai": {"name": "Shanghai", "bbox": (31.0, 121.2, 31.5, 121.8)},
    "london":   {"name": "London",   "bbox": (51.45, -0.25, 51.55, 0.00)},
    "new_york": {"name": "New York", "bbox": (40.65, -74.05, 40.80, -73.90)},
}

DATA_LAKE_BASE = "/opt/airflow/data"
OVERPASS_URLS = [
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
REQUEST_TIMEOUT_SECONDS = 60
OVERPASS_QUERY_TIMEOUT_SECONDS = 45
RETRY_WAIT_SECONDS = 5
CITY_WAIT_SECONDS = 3


def build_query(city_info, target_date):
    s, w, n, e = city_info["bbox"]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    date_setting = ""
    if target_date < today:
        date_setting = f'[date:"{target_overpass_datetime(target_date)}"]'
    elif target_date > today:
        raise ValueError("target_date cannot be in the future for this pipeline")

    return f"""
    [out:json][timeout:{OVERPASS_QUERY_TIMEOUT_SECONDS}]{date_setting};
    (
      node["amenity"="restaurant"]({s},{w},{n},{e});
      node["amenity"="cafe"]({s},{w},{n},{e});
      node["amenity"="fast_food"]({s},{w},{n},{e});
    );
    out body;
    """


def fetch_restaurants(city_key, city_info, target_date):
    query = build_query(city_info, target_date)
    last_error = None

    for attempt in range(1, 4):
        for url in OVERPASS_URLS:
            try:
                response = requests.post(
                    url,
                    data={"data": query},
                    headers={
                        "Accept": "application/json",
                        "User-Agent": "BigDataProject/1.0 (student project; isep.fr)",
                    },
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                data = response.json()
                data["_metadata"] = {
                    "city_key": city_key,
                    "city_name": city_info["name"],
                    "ingested_at": (
                        target_datetime_iso(target_date)
                        if target_date < datetime.now(timezone.utc).strftime("%Y-%m-%d")
                        else datetime.now(timezone.utc).isoformat()
                    ),
                    "source": "Overpass API (OpenStreetMap)",
                    "overpass_url": url,
                    "target_date": target_date,
                    "total_elements": len(data.get("elements", [])),
                }
                return data
            except Exception as e:
                last_error = e
                print(
                    f"[Restaurants] Attempt {attempt}/3 failed for "
                    f"{city_info['name']} via {url}: {e}"
                )

        if attempt < 3:
            time.sleep(RETRY_WAIT_SECONDS * attempt)

    raise RuntimeError(f"Could not fetch {city_info['name']} after retries: {last_error}")


def save_raw(city_key, data, date_str):
    path = os.path.join(DATA_LAKE_BASE, "raw", "restaurants", date_str)
    os.makedirs(path, exist_ok=True)
    filename = os.path.join(path, f"{city_key}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[Restaurants] Saved: {filename} ({data['_metadata']['total_elements']} items)")
    return filename


def run_ingestion(**kwargs):
    date_str = get_target_date(kwargs)
    results = []
    for city_key, city_info in CITIES.items():
        try:
            print(f"[Restaurants] Fetching {city_info['name']}...")
            data = fetch_restaurants(city_key, city_info, date_str)
            save_raw(city_key, data, date_str)
            results.append({"city": city_key, "status": "success", "count": data["_metadata"]["total_elements"]})
        except Exception as e:
            print(f"[Restaurants] ERROR {city_key}: {e}")
            results.append({"city": city_key, "status": "error", "error": str(e)})

        time.sleep(CITY_WAIT_SECONDS)

    failed = [r for r in results if r["status"] != "success"]
    if failed:
        raise RuntimeError(f"Restaurant ingestion failed for {len(failed)} city/cities: {failed}")

    print(f"[Restaurants] Ingestion complete: {results}")
    return results


if __name__ == "__main__":
    run_ingestion()
