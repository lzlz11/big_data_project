"""
Kibana Setup Script
Automatically imports index pattern and dashboard into Kibana
Run this after docker-compose up and all services are healthy
"""

import requests
import time
import json
import os

KIBANA_URL = "http://localhost:5601"
ES_URL = "http://localhost:9200"


def wait_for_service(url: str, name: str, max_retries: int = 30):
    """Wait for a service to become available."""
    print(f"[Setup] Waiting for {name} to be ready...")
    for i in range(max_retries):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code < 500:
                print(f"[Setup] ✅ {name} is ready!")
                return True
        except Exception:
            pass
        print(f"[Setup] ... retry {i+1}/{max_retries}")
        time.sleep(5)
    print(f"[Setup] ❌ {name} not available after {max_retries} retries")
    return False


def create_index_pattern():
    """Create Kibana index pattern for city_dining_score."""
    print("[Setup] Creating Kibana index pattern...")

    payload = {
        "attributes": {
            "title": "city_dining_score*",
            "timeFieldName": "ingestion_date",
        }
    }

    headers = {
        "kbn-xsrf": "true",
        "Content-Type": "application/json",
    }

    r = requests.post(
        f"{KIBANA_URL}/api/saved_objects/index-pattern/city_dining_score",
        json=payload,
        headers=headers,
        timeout=30,
    )

    if r.status_code in (200, 201, 409):
        print(f"[Setup] ✅ Index pattern created (status: {r.status_code})")
        return True
    else:
        print(f"[Setup] ⚠️  Index pattern: {r.status_code} - {r.text[:200]}")
        return False


def import_dashboard():
    """Import dashboard from ndjson file."""
    dashboard_path = os.path.join(
        os.path.dirname(__file__), "dashboard_export.ndjson"
    )

    if not os.path.exists(dashboard_path):
        print("[Setup] ⚠️  Dashboard file not found, skipping.")
        return False

    print("[Setup] Importing Kibana dashboard...")

    with open(dashboard_path, "rb") as f:
        r = requests.post(
            f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true",
            headers={"kbn-xsrf": "true"},
            files={"file": ("dashboard_export.ndjson", f, "application/ndjson")},
            timeout=30,
        )

    if r.status_code == 200:
        result = r.json()
        print(f"[Setup] ✅ Dashboard imported: {result.get('successCount', 0)} objects")
        return True
    else:
        print(f"[Setup] ⚠️  Dashboard import: {r.status_code} - {r.text[:200]}")
        return False


def set_default_index():
    """Set city_dining_score as default index pattern."""
    headers = {"kbn-xsrf": "true", "Content-Type": "application/json"}

    r = requests.post(
        f"{KIBANA_URL}/api/kibana/settings",
        json={"changes": {"defaultIndex": "city_dining_score"}},
        headers=headers,
        timeout=10,
    )
    if r.status_code == 200:
        print("[Setup] ✅ Default index pattern set.")


def print_summary():
    print("\n" + "="*60)
    print("🎉  SETUP COMPLETE!")
    print("="*60)
    print(f"  Airflow:       http://localhost:8080  (admin / admin)")
    print(f"  Kibana:        http://localhost:5601")
    print(f"  Elasticsearch: http://localhost:9200")
    print("="*60)
    print("\nNext steps:")
    print("  1. Open Airflow → enable 'weather_restaurant_pipeline' DAG")
    print("  2. Click ▶ to trigger a manual run")
    print("  3. Open Kibana → Dashboard → '🍽️ City Dining Score'")
    print("="*60 + "\n")


if __name__ == "__main__":
    if not wait_for_service(f"{KIBANA_URL}/api/status", "Kibana"):
        exit(1)
    if not wait_for_service(f"{ES_URL}/_cluster/health", "Elasticsearch"):
        exit(1)

    time.sleep(10)  # Extra wait for Kibana to fully initialize

    create_index_pattern()
    import_dashboard()
    set_default_index()
    print_summary()
