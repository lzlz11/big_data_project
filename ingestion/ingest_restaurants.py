"""
Ingestion Script - Restaurant Data
Source: Overpass API (OpenStreetMap) - free, no key required
Cities: Paris, Shanghai, London, New York
"""

import requests
import json
import os
from datetime import datetime, timezone

CITIES = {
    "paris":    {"name": "Paris",    "bbox": (48.815, 2.224, 48.902, 2.470)},
    "shanghai": {"name": "Shanghai", "bbox": (31.0, 121.2, 31.5, 121.8)},
    "london":   {"name": "London",   "bbox": (51.45, -0.25, 51.55, 0.00)},
    "new_york": {"name": "New York", "bbox": (40.65, -74.05, 40.80, -73.90)},
}

DATA_LAKE_BASE = "/opt/airflow/data"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def fetch_restaurants(city_key, city_info):
    s, w, n, e = city_info["bbox"]
    query = f"""
    [out:json][timeout:60];
    (
      node["amenity"="restaurant"]({s},{w},{n},{e});
      node["amenity"="cafe"]({s},{w},{n},{e});
      node["amenity"="fast_food"]({s},{w},{n},{e});
    );
    out body;
    """
    response = requests.post(
        OVERPASS_URL,
        data={"data": query},
        headers={
            "Accept": "application/json",
            "User-Agent": "BigDataProject/1.0 (student project; isep.fr)",
        },
        timeout=90,
    )
    response.raise_for_status()
    data = response.json()
    data["_metadata"] = {
        "city_key": city_key,
        "city_name": city_info["name"],
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "Overpass API (OpenStreetMap)",
        "total_elements": len(data.get("elements", [])),
    }
    return data


def save_raw(city_key, data, date_str):
    path = os.path.join(DATA_LAKE_BASE, "raw", "restaurants", date_str)
    os.makedirs(path, exist_ok=True)
    filename = os.path.join(path, f"{city_key}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[Restaurants] Saved: {filename} ({data['_metadata']['total_elements']} items)")
    return filename


def run_ingestion(**kwargs):
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    results = []
    for city_key, city_info in CITIES.items():
        try:
            print(f"[Restaurants] Fetching {city_info['name']}...")
            data = fetch_restaurants(city_key, city_info)
            save_raw(city_key, data, date_str)
            results.append({"city": city_key, "status": "success", "count": data["_metadata"]["total_elements"]})
        except Exception as e:
            print(f"[Restaurants] ERROR {city_key}: {e}")
            results.append({"city": city_key, "status": "error", "error": str(e)})
    return results


if __name__ == "__main__":
    run_ingestion()
