# Weather x Restaurant - City Dining Score
## Big Data Project | ISEP

> Analyse quotidienne des conditions météo et de la densité de restaurants
> dans 4 villes mondiales pour produire un score "Meilleure ville où dîner aujourd'hui"

---

## Villes analysées
Paris | Shanghai | London | New York

## Sources de données
- Open-Meteo API (gratuit, sans clé, toutes les heures)
- Overpass API / OpenStreetMap (gratuit, sans clé, quotidien)

## Architecture
Airflow DAG (06:00 UTC)
  1. ingest_weather + ingest_restaurants (parallel)
  2. format_to_postgres
  3. dbt run (stg_weather, stg_restaurants, mart_city_dining_score, mart_weekly_trend, mart_cuisine_analysis)
  4. dbt test
  5. index_to_elasticsearch (3 indexes)
  => Kibana Dashboard

## Demarrage rapide (Windows)
Double-cliquer start.bat

Ou manuellement:
  docker-compose up -d
  python kibana/setup_kibana.py

## Interfaces
- Airflow:       http://localhost:8080  (admin / admin)
- Kibana:        http://localhost:5601
- Elasticsearch: http://localhost:9200

## Formule Dining Score (0-10)
dining_score = outdoor_comfort x 40% + venue_density x 30% + cuisine_diversity x 30%

## Checklist points
- Ingestion 2 sources    : 2 pts
- Formatting DBT         : 2 pts
- Combination JOIN DBT   : 2 pts
- Indexation ES          : 2 pts
- Dashboard Kibana       : 2 pts
- DBT instead of Spark   : 1.5 pts
- Interesting output     : 1.5 pts
- Naming conventions     : 1 pt
- One-click DAG          : 1 pt
Total                    : ~17 pts
