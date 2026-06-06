@echo off
echo ============================================================
echo   Big Data Project - Full Reset and Fix
echo ============================================================

set SCHEDULER=big_data_project-airflow-scheduler-1
set POSTGRES=big_data_project-postgres-1

echo [1/3] Resetting database...
docker exec %POSTGRES% psql -U airflow -c "DROP SCHEMA IF EXISTS dbt_weather_restaurants CASCADE;"
docker exec %POSTGRES% psql -U airflow -c "CREATE SCHEMA dbt_weather_restaurants;"

echo [2/3] Copying all files...
docker cp ingestion\ingest_weather.py %SCHEDULER%:/opt/airflow/ingestion/ingest_weather.py
docker cp ingestion\ingest_restaurants.py %SCHEDULER%:/opt/airflow/ingestion/ingest_restaurants.py
docker cp ingestion\format_to_postgres.py %SCHEDULER%:/opt/airflow/ingestion/format_to_postgres.py
docker cp indexing\index_to_elasticsearch.py %SCHEDULER%:/opt/airflow/indexing/index_to_elasticsearch.py
docker cp dbt_project\models\staging\stg_weather.sql %SCHEDULER%:/opt/airflow/dbt_project/models/staging/stg_weather.sql
docker cp dbt_project\models\staging\stg_restaurants.sql %SCHEDULER%:/opt/airflow/dbt_project/models/staging/stg_restaurants.sql
docker cp dbt_project\models\marts\mart_city_dining_score.sql %SCHEDULER%:/opt/airflow/dbt_project/models/marts/mart_city_dining_score.sql
docker cp dbt_project\models\marts\mart_cuisine_analysis.sql %SCHEDULER%:/opt/airflow/dbt_project/models/marts/mart_cuisine_analysis.sql
docker cp dbt_project\models\marts\mart_weekly_trend.sql %SCHEDULER%:/opt/airflow/dbt_project/models/marts/mart_weekly_trend.sql
docker cp dbt_project\models\schema.yml %SCHEDULER%:/opt/airflow/dbt_project/models/schema.yml
docker cp dbt_project\dbt_project.yml %SCHEDULER%:/opt/airflow/dbt_project/dbt_project.yml
docker cp dbt_project\profiles.yml %SCHEDULER%:/opt/airflow/dbt_project/profiles.yml
docker cp dbt_project\tests\assert_dining_score_range.sql %SCHEDULER%:/opt/airflow/dbt_project/tests/assert_dining_score_range.sql
docker cp dbt_project\tests\assert_all_cities_present.sql %SCHEDULER%:/opt/airflow/dbt_project/tests/assert_all_cities_present.sql
docker cp dags\weather_restaurant_dag.py %SCHEDULER%:/opt/airflow/dags/weather_restaurant_dag.py
docker cp dags\weather_restaurant_dag.py big_data_project-airflow-webserver-1:/opt/airflow/dags/weather_restaurant_dag.py

echo [3/3] Done!
echo.
echo Go to http://localhost:8080 and trigger the DAG
echo.
pause
