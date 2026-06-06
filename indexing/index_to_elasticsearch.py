"""
Indexing Script - Push DBT mart outputs to Elasticsearch
"""

import psycopg2
import psycopg2.extras
from elasticsearch import Elasticsearch

DB_CONFIG = {"host": "postgres", "port": 5432, "dbname": "airflow", "user": "airflow", "password": "airflow"}
SCHEMA = "dbt_weather_restaurants"
ES_HOST = "http://elasticsearch:9200"

INDEXES = {
    "city_dining_score": {
        "table": "mart_city_dining_score",
        "id_fields": ["city_key", "ingestion_date"],
        "mappings": {
            "city_key": {"type": "keyword"}, "city_name": {"type": "keyword"},
            "ingestion_date": {"type": "date"}, "ingested_at_utc": {"type": "date"},
            "temperature_c": {"type": "float"}, "humidity_pct": {"type": "float"},
            "precipitation_mm": {"type": "float"}, "wind_speed_kmh": {"type": "float"},
            "weather_description": {"type": "keyword"}, "weather_category": {"type": "keyword"},
            "outdoor_comfort_score": {"type": "float"}, "total_venues": {"type": "integer"},
            "restaurant_count": {"type": "integer"}, "cafe_count": {"type": "integer"},
            "fast_food_count": {"type": "integer"}, "cuisine_diversity": {"type": "integer"},
            "venue_density_score": {"type": "float"}, "cuisine_diversity_score": {"type": "float"},
            "dining_score": {"type": "float"},
            "dining_recommendation": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "city_rank_today": {"type": "integer"},
        }
    },
    "city_weekly_trend": {
        "table": "mart_weekly_trend",
        "id_fields": ["city_key", "ingestion_date"],
        "mappings": {
            "city_key": {"type": "keyword"}, "city_name": {"type": "keyword"},
            "ingestion_date": {"type": "date"}, "dining_score": {"type": "float"},
            "outdoor_comfort_score": {"type": "float"}, "temperature_c": {"type": "float"},
            "precipitation_mm": {"type": "float"}, "weather_category": {"type": "keyword"},
            "score_change": {"type": "float"}, "avg_score_7d": {"type": "float"},
        }
    },
    "city_cuisine_analysis": {
        "table": "mart_cuisine_analysis",
        "id_fields": ["city_key", "cuisine", "place_type", "ingestion_date"],
        "mappings": {
            "city_key": {"type": "keyword"}, "city_name": {"type": "keyword"},
            "cuisine": {"type": "keyword"}, "place_type": {"type": "keyword"},
            "venue_count": {"type": "integer"}, "ingestion_date": {"type": "date"},
            "cuisine_rank": {"type": "integer"}, "share_pct": {"type": "float"},
        }
    },
}


def run_indexing(**kwargs):
    conn = psycopg2.connect(**DB_CONFIG)
    es = Elasticsearch(ES_HOST)
    total = 0
    try:
        for index_name, config in INDEXES.items():
            if not es.indices.exists(index=index_name):
                es.indices.create(index=index_name, body={"mappings": {"properties": config["mappings"]}})
                print(f"[ES] Created index: {index_name}")

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(f"SELECT * FROM {SCHEMA}.{config['table']};")
                rows = [dict(r) for r in cur.fetchall()]

            if not rows:
                print(f"[ES] No rows for {index_name}")
                continue

            actions = []
            for row in rows:
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat()
                doc_id = "_".join(str(row.get(f, "")) for f in config["id_fields"])
                actions.append({"index": {"_index": index_name, "_id": doc_id}})
                actions.append(row)

            es.bulk(body=actions)
            print(f"[ES] Indexed {len(rows)} docs → {index_name}")
            total += len(rows)

        print(f"[ES] Done: {total} total documents.")
    finally:
        conn.close()


if __name__ == "__main__":
    run_indexing()
