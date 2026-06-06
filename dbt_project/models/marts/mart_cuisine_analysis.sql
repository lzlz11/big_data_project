-- Cuisine diversity analysis per city

{{ config(materialized='table') }}

WITH restaurants AS (
    SELECT * FROM {{ ref('stg_restaurants') }}
    WHERE ingestion_date = CURRENT_DATE
      AND cuisine != 'unknown'
),

counts AS (
    SELECT
        city_key, city_name, cuisine, place_type,
        COUNT(*) AS venue_count,
        MAX(ingestion_date) AS ingestion_date
    FROM restaurants
    GROUP BY city_key, city_name, cuisine, place_type
),

ranked AS (
    SELECT *,
        RANK() OVER (PARTITION BY city_key ORDER BY venue_count DESC) AS cuisine_rank,
        ROUND(venue_count::numeric / SUM(venue_count) OVER (PARTITION BY city_key) * 100, 1) AS share_pct
    FROM counts
)

SELECT * FROM ranked
WHERE cuisine_rank <= 15
ORDER BY city_key, cuisine_rank
