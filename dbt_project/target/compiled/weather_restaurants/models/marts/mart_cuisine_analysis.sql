-- Cuisine diversity analysis per city



WITH restaurants AS (
    SELECT * FROM "airflow"."dbt_weather_restaurants"."stg_restaurants"
    WHERE cuisine != 'unknown'
),

counts AS (
    SELECT
        city_key, city_name, ingestion_date, cuisine, place_type,
        COUNT(*) AS venue_count
    FROM restaurants
    GROUP BY city_key, city_name, ingestion_date, cuisine, place_type
),

ranked AS (
    SELECT *,
        RANK() OVER (PARTITION BY city_key, ingestion_date ORDER BY venue_count DESC) AS cuisine_rank,
        ROUND(venue_count::numeric / SUM(venue_count) OVER (PARTITION BY city_key, ingestion_date) * 100, 1) AS share_pct
    FROM counts
)

SELECT * FROM ranked
WHERE cuisine_rank <= 15
ORDER BY ingestion_date DESC, city_key, cuisine_rank