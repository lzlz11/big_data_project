"""
Format Script - Load raw JSON into PostgreSQL for DBT
"""

import json
import os
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

DATA_LAKE_BASE = "/opt/airflow/data"
DB_CONFIG = {"host": "postgres", "port": 5432, "dbname": "airflow", "user": "airflow", "password": "airflow"}
SCHEMA = "dbt_weather_restaurants"


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def setup_schema(conn):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.weather_raw (
                id SERIAL PRIMARY KEY, city_key TEXT, city_name TEXT,
                ingested_at TIMESTAMPTZ, current_temperature FLOAT,
                current_humidity FLOAT, current_precipitation FLOAT,
                current_wind_speed FLOAT, current_weather_code INTEGER,
                raw_json JSONB, loaded_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.restaurants_raw (
                id SERIAL PRIMARY KEY, osm_id BIGINT, city_key TEXT,
                city_name TEXT, name TEXT, amenity TEXT, cuisine TEXT,
                latitude FLOAT, longitude FLOAT, opening_hours TEXT,
                ingested_at TIMESTAMPTZ, loaded_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()
    print("[Format] Schema ready.")


def load_weather(conn, date_str):
    path = os.path.join(DATA_LAKE_BASE, "raw", "weather", date_str)
    if not os.path.exists(path):
        return 0
    count = 0
    with conn.cursor() as cur:
        for f in os.listdir(path):
            if not f.endswith(".json"): continue
            with open(os.path.join(path, f)) as fp:
                data = json.load(fp)
            meta = data.get("_metadata", {})
            current = data.get("current", {})
            cur.execute(f"""
                INSERT INTO {SCHEMA}.weather_raw
                (city_key, city_name, ingested_at, current_temperature,
                 current_humidity, current_precipitation, current_wind_speed,
                 current_weather_code, raw_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
            """, (meta.get("city_key"), meta.get("city_name"), meta.get("ingested_at"),
                  current.get("temperature_2m"), current.get("relative_humidity_2m"),
                  current.get("precipitation"), current.get("wind_speed_10m"),
                  current.get("weather_code"), json.dumps(data)))
            count += 1
    conn.commit()
    print(f"[Format] Loaded {count} weather records.")
    return count


def load_restaurants(conn, date_str):
    path = os.path.join(DATA_LAKE_BASE, "raw", "restaurants", date_str)
    if not os.path.exists(path):
        return 0
    total = 0
    with conn.cursor() as cur:
        for f in os.listdir(path):
            if not f.endswith(".json"): continue
            with open(os.path.join(path, f)) as fp:
                data = json.load(fp)
            meta = data.get("_metadata", {})
            rows = []
            for el in data.get("elements", []):
                tags = el.get("tags", {})
                rows.append((el.get("id"), meta.get("city_key"), meta.get("city_name"),
                              tags.get("name"), tags.get("amenity"), tags.get("cuisine"),
                              el.get("lat"), el.get("lon"), tags.get("opening_hours"),
                              meta.get("ingested_at")))
            if rows:
                psycopg2.extras.execute_values(cur, f"""
                    INSERT INTO {SCHEMA}.restaurants_raw
                    (osm_id, city_key, city_name, name, amenity, cuisine,
                     latitude, longitude, opening_hours, ingested_at)
                    VALUES %s ON CONFLICT DO NOTHING;
                """, rows)
            total += len(rows)
    conn.commit()
    print(f"[Format] Loaded {total} restaurant records.")
    return total


def run_formatting(**kwargs):
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = get_connection()
    try:
        setup_schema(conn)
        load_weather(conn, date_str)
        load_restaurants(conn, date_str)
    finally:
        conn.close()


if __name__ == "__main__":
    run_formatting()
