"""
Ingestion Script - Restaurant Data
Source: Overpass API (OpenStreetMap) - free, no key required
Cities: Paris, Shanghai, London, New York
"""

import requests
import json
import os
import time
import math
import random
from datetime import datetime, timezone
from ingestion.date_utils import get_target_dates, target_datetime_iso, target_overpass_datetime
from ingestion.object_storage import upload_json
CITIES = {
    "paris":    {"name": "Paris",    "bbox": (48.815, 2.224, 48.902, 2.470)},
    "shanghai": {"name": "Shanghai", "bbox": (31.0, 121.2, 31.5, 121.8)},
    "london":   {"name": "London",   "bbox": (51.45, -0.25, 51.55, 0.00)},
    "new_york": {"name": "New York", "bbox": (40.65, -74.05, 40.80, -73.90)},
}

DATA_LAKE_BASE = "/opt/airflow/data"
FIXED_SAMPLE_DIR = os.path.join(DATA_LAKE_BASE, "sample", "restaurants")
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
REQUEST_TIMEOUT_SECONDS = 20
OVERPASS_QUERY_TIMEOUT_SECONDS = 25
RETRY_WAIT_SECONDS = 5
CITY_WAIT_SECONDS = 3
MAX_RESTAURANTS_PER_CITY = int(os.getenv("MAX_RESTAURANTS_PER_CITY", "100"))
SAMPLE_GRID_SPLITS = int(os.getenv("SAMPLE_GRID_SPLITS", "3"))
SAMPLE_MODE = os.getenv("SAMPLE_MODE", "limit")
USE_FIXED_RESTAURANT_SAMPLE = os.getenv("USE_FIXED_RESTAURANT_SAMPLE", "true").lower() == "true"


def build_query(city_info, target_date, bbox=None, limit=None):
    s, w, n, e = bbox or city_info["bbox"]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    date_setting = ""
    if target_date < today:
        date_setting = f'[date:"{target_overpass_datetime(target_date)}"]'
    elif target_date > today:
        raise ValueError("target_date cannot be in the future for this pipeline")

    limit_clause = f" {limit}" if limit else ""
    return f"""
    [out:json][timeout:{OVERPASS_QUERY_TIMEOUT_SECONDS}]{date_setting};
    (
      node["amenity"="restaurant"]({s},{w},{n},{e});
      node["amenity"="cafe"]({s},{w},{n},{e});
      node["amenity"="fast_food"]({s},{w},{n},{e});
    );
    out body{limit_clause};
    """


def build_id_query(city_info, target_date, node_ids):
    if not node_ids:
        raise RuntimeError(f"No fixed restaurant ids are available for {city_info['name']}")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    date_setting = ""
    if target_date < today:
        date_setting = f'[date:"{target_overpass_datetime(target_date)}"]'
    elif target_date > today:
        raise ValueError("target_date cannot be in the future for this pipeline")

    ids = ",".join(str(node_id) for node_id in node_ids)
    return f"""
    [out:json][timeout:{OVERPASS_QUERY_TIMEOUT_SECONDS}]{date_setting};
    (
      node(id:{ids})["amenity"~"^(restaurant|cafe|fast_food)$"];
    );
    out body;
    """


def request_overpass(city_name, query):
    last_error = None

    for url in OVERPASS_URLS:
        for attempt in range(1, 4):
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
                return response.json(), url
            except Exception as e:
                last_error = e
                print(
                    f"[Restaurants] Attempt {attempt}/3 failed for "
                    f"{city_name} via {url}: {e}"
                )

            if attempt < 3:
                time.sleep(RETRY_WAIT_SECONDS * attempt)

    raise RuntimeError(f"Could not fetch {city_name} after retries: {last_error}")


def stable_sort_elements(elements):
    return sorted(elements, key=lambda element: (element.get("type", ""), element.get("id", 0)))


def fixed_sample_path(city_key):
    return os.path.join(FIXED_SAMPLE_DIR, f"{city_key}.json")


def load_fixed_sample_ids(city_key):
    path = fixed_sample_path(city_key)
    if not os.path.exists(path):
        return []

    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("node_ids", [])


def save_fixed_sample_ids(city_key, city_info, target_date, elements):
    os.makedirs(FIXED_SAMPLE_DIR, exist_ok=True)
    node_ids = [
        element["id"]
        for element in stable_sort_elements(elements)
        if element.get("type") == "node" and element.get("id") is not None
    ][:MAX_RESTAURANTS_PER_CITY]

    payload = {
        "city_key": city_key,
        "city_name": city_info["name"],
        "created_from_target_date": target_date,
        "sample_limit": MAX_RESTAURANTS_PER_CITY,
        "node_ids": node_ids,
    }

    with open(fixed_sample_path(city_key), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[Restaurants] Fixed sample saved for {city_info['name']}: {len(node_ids)} ids")
    return node_ids


def split_bbox(bbox, splits):
    s, w, n, e = bbox
    lat_step = (n - s) / splits
    lon_step = (e - w) / splits
    cells = []
    for lat_idx in range(splits):
        for lon_idx in range(splits):
            cell_s = s + lat_step * lat_idx
            cell_n = s + lat_step * (lat_idx + 1)
            cell_w = w + lon_step * lon_idx
            cell_e = w + lon_step * (lon_idx + 1)
            cells.append((cell_s, cell_w, cell_n, cell_e))
    return cells


def add_unique_elements(target, seen_ids, elements):
    for element in elements:
        osm_id = element.get("id")
        if osm_id in seen_ids:
            continue
        seen_ids.add(osm_id)
        target.append(element)


def fetch_grid_sampled_restaurants(city_key, city_info, target_date):
    limit = MAX_RESTAURANTS_PER_CITY
    cells = split_bbox(city_info["bbox"], SAMPLE_GRID_SPLITS)
    rng = random.Random(f"{target_date}:{city_key}")
    rng.shuffle(cells)

    elements = []
    seen_ids = set()
    used_urls = []
    per_cell_limit = max(1, math.ceil(limit / len(cells)) + 3)

    for cell in cells:
        remaining = limit - len(elements)
        if remaining <= 0:
            break

        query_limit = min(per_cell_limit, remaining)
        query = build_query(city_info, target_date, bbox=cell, limit=query_limit)
        cell_data, overpass_url = request_overpass(city_info["name"], query)
        used_urls.append(overpass_url)
        add_unique_elements(elements, seen_ids, cell_data.get("elements", []))
        time.sleep(1)

    if len(elements) > limit:
        elements = rng.sample(elements, limit)

    return {
        "version": 0.6,
        "generator": "Overpass API sampled by project pipeline",
        "elements": elements,
        "_sample": {
            "strategy": "random_grid_bbox_sample",
            "grid_splits": SAMPLE_GRID_SPLITS,
            "max_restaurants_per_city": limit,
            "per_cell_limit": per_cell_limit,
            "queried_cells": len(used_urls),
        },
        "_overpass_urls": sorted(set(used_urls)),
    }


def fetch_limited_restaurants(city_info, target_date):
    query = build_query(city_info, target_date, limit=MAX_RESTAURANTS_PER_CITY)
    data, overpass_url = request_overpass(city_info["name"], query)
    data["_overpass_urls"] = [overpass_url]
    data["elements"] = stable_sort_elements(data.get("elements", []))[:MAX_RESTAURANTS_PER_CITY]
    data["_sample"] = {
        "strategy": "overpass_limit_first_n",
        "max_restaurants_per_city": MAX_RESTAURANTS_PER_CITY,
    }
    return data


def fetch_fixed_restaurant_sample(city_key, city_info, target_date):
    node_ids = load_fixed_sample_ids(city_key)

    if len(node_ids) < MAX_RESTAURANTS_PER_CITY:
        seed_data = fetch_limited_restaurants(city_info, target_date)
        node_ids = save_fixed_sample_ids(city_key, city_info, target_date, seed_data.get("elements", []))
        seed_data["_sample"] = {
            "strategy": "fixed_osm_node_id_panel_created",
            "sample_file": fixed_sample_path(city_key),
            "sample_limit": MAX_RESTAURANTS_PER_CITY,
            "requested_ids": len(node_ids),
            "returned_elements": len(seed_data.get("elements", [])),
        }
        return seed_data

    node_ids = node_ids[:MAX_RESTAURANTS_PER_CITY]

    query = build_id_query(city_info, target_date, node_ids)
    data, overpass_url = request_overpass(city_info["name"], query)
    returned_ids = {element.get("id") for element in data.get("elements", [])}
    ordered_elements = []
    element_by_id = {element.get("id"): element for element in data.get("elements", [])}

    for node_id in node_ids:
        if node_id in returned_ids:
            ordered_elements.append(element_by_id[node_id])

    data["_overpass_urls"] = [overpass_url]
    data["elements"] = ordered_elements
    data["_sample"] = {
        "strategy": "fixed_osm_node_id_panel",
        "sample_file": fixed_sample_path(city_key),
        "sample_limit": MAX_RESTAURANTS_PER_CITY,
        "requested_ids": len(node_ids),
        "returned_elements": len(ordered_elements),
    }
    return data


def fetch_full_restaurants(city_info, target_date):
    query = build_query(city_info, target_date)
    data, overpass_url = request_overpass(city_info["name"], query)
    data["_overpass_urls"] = [overpass_url]
    return data


def fetch_restaurants(city_key, city_info, target_date):
    if MAX_RESTAURANTS_PER_CITY > 0:
        if USE_FIXED_RESTAURANT_SAMPLE:
            data = fetch_fixed_restaurant_sample(city_key, city_info, target_date)
        elif SAMPLE_MODE == "grid":
            data = fetch_grid_sampled_restaurants(city_key, city_info, target_date)
        else:
            data = fetch_limited_restaurants(city_info, target_date)
    else:
        data = fetch_full_restaurants(city_info, target_date)

    data["_metadata"] = {
        "city_key": city_key,
        "city_name": city_info["name"],
        "ingested_at": (
            target_datetime_iso(target_date)
            if target_date < datetime.now(timezone.utc).strftime("%Y-%m-%d")
            else datetime.now(timezone.utc).isoformat()
        ),
        "source": "Overpass API (OpenStreetMap)",
        "overpass_urls": data.get("_overpass_urls", []),
        "target_date": target_date,
        "sampled": MAX_RESTAURANTS_PER_CITY > 0,
        "sample_mode": (
            "fixed"
            if USE_FIXED_RESTAURANT_SAMPLE and MAX_RESTAURANTS_PER_CITY > 0
            else SAMPLE_MODE if MAX_RESTAURANTS_PER_CITY > 0
            else "full"
        ),
        "sample_limit": MAX_RESTAURANTS_PER_CITY if MAX_RESTAURANTS_PER_CITY > 0 else None,
        "total_elements": len(data.get("elements", [])),
    }
    return data


def save_raw(city_key, data, date_str):
    path = os.path.join(DATA_LAKE_BASE, "raw", "restaurants", date_str)
    os.makedirs(path, exist_ok=True)
    filename = os.path.join(path, f"{city_key}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    upload_json("restaurants", date_str, city_key, data)
    
    print(f"[Restaurants] Saved: {filename} ({data['_metadata']['total_elements']} items)")
    return filename


def run_ingestion(**kwargs):
    target_dates = get_target_dates(kwargs)
    results = []

    for date_str in target_dates:
        print(f"[Restaurants] Processing target date: {date_str}")
        for city_key, city_info in CITIES.items():
            try:
                print(f"[Restaurants] Fetching {city_info['name']} on {date_str}...")
                data = fetch_restaurants(city_key, city_info, date_str)
                save_raw(city_key, data, date_str)
                results.append({
                    "date": date_str,
                    "city": city_key,
                    "status": "success",
                    "count": data["_metadata"]["total_elements"],
                })
            except Exception as e:
                print(f"[Restaurants] ERROR {city_key} on {date_str}: {e}")
                results.append({
                    "date": date_str,
                    "city": city_key,
                    "status": "error",
                    "error": str(e),
                })

            time.sleep(CITY_WAIT_SECONDS)

    failed = [r for r in results if r["status"] != "success"]
    if failed:
        raise RuntimeError(f"Restaurant ingestion failed for {len(failed)} city/cities: {failed}")

    print(f"[Restaurants] Ingestion complete: {results}")
    return results


if __name__ == "__main__":
    run_ingestion()
