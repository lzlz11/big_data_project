-- Weekly trend: dining scores over 7 days



WITH daily AS (
    SELECT * FROM "airflow"."dbt_weather_restaurants"."mart_city_dining_score"
    WHERE ingestion_date >= (
        SELECT MAX(ingestion_date) - INTERVAL '7 days'
        FROM "airflow"."dbt_weather_restaurants"."mart_city_dining_score"
    )
)

SELECT *,
    dining_score - LAG(dining_score) OVER (PARTITION BY city_key ORDER BY ingestion_date) AS score_change,
    AVG(dining_score) OVER (PARTITION BY city_key ORDER BY ingestion_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_score_7d,
    AVG(air_quality_score) OVER (PARTITION BY city_key ORDER BY ingestion_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_air_quality_score_7d
FROM daily
ORDER BY ingestion_date DESC, city_key