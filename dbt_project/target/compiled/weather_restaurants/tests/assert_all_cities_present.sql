SELECT ingestion_date, COUNT(DISTINCT city_key) AS city_count
FROM "airflow"."dbt_weather_restaurants"."mart_city_dining_score"
WHERE ingestion_date = CURRENT_DATE
GROUP BY ingestion_date
HAVING COUNT(DISTINCT city_key) < 4