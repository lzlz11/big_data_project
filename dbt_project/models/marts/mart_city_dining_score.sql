-- MAIN OUTPUT: City Dining Score
-- Question: Which city is best to dine out today based on weather + restaurant availability?

{{ config(materialized='table') }}

WITH weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
    WHERE ingestion_date = CURRENT_DATE
),

restaurant_stats AS (
    SELECT
        city_key,
        city_name,
        ingestion_date,
        COUNT(*)                                            AS total_venues,
        COUNT(*) FILTER (WHERE place_type = 'restaurant')  AS restaurant_count,
        COUNT(*) FILTER (WHERE place_type = 'cafe')        AS cafe_count,
        COUNT(*) FILTER (WHERE place_type = 'fast_food')   AS fast_food_count,
        COUNT(DISTINCT cuisine)
            FILTER (WHERE cuisine != 'unknown')            AS cuisine_diversity
    FROM {{ ref('stg_restaurants') }}
    WHERE ingestion_date = CURRENT_DATE
    GROUP BY city_key, city_name, ingestion_date
),

combined AS (
    SELECT
        w.city_key,
        w.city_name,
        w.ingestion_date,
        w.ingested_at_utc,
        w.temperature_c,
        w.humidity_pct,
        w.precipitation_mm,
        w.wind_speed_kmh,
        w.weather_description,
        w.weather_category,
        w.outdoor_comfort_score,
        COALESCE(r.total_venues, 0)        AS total_venues,
        COALESCE(r.restaurant_count, 0)    AS restaurant_count,
        COALESCE(r.cafe_count, 0)          AS cafe_count,
        COALESCE(r.fast_food_count, 0)     AS fast_food_count,
        COALESCE(r.cuisine_diversity, 0)   AS cuisine_diversity,
        ROUND(LEAST(COALESCE(r.total_venues, 0)::numeric / 100.0, 1.0) * 10, 1) AS venue_density_score,
        ROUND(LEAST(COALESCE(r.cuisine_diversity, 0)::numeric / 20.0, 1.0) * 10, 1) AS cuisine_diversity_score
    FROM weather w
    LEFT JOIN restaurant_stats r USING (city_key)
),

final AS (
    SELECT *,
        ROUND(
            (outdoor_comfort_score * 0.40)
            + (venue_density_score * 0.30)
            + (cuisine_diversity_score * 0.30)
        , 2) AS dining_score,
        CASE
            WHEN (outdoor_comfort_score*0.4 + venue_density_score*0.3 + cuisine_diversity_score*0.3) >= 7
                THEN 'Excellent day to dine out!'
            WHEN (outdoor_comfort_score*0.4 + venue_density_score*0.3 + cuisine_diversity_score*0.3) >= 5
                THEN 'Good day - consider outdoor seating'
            WHEN (outdoor_comfort_score*0.4 + venue_density_score*0.3 + cuisine_diversity_score*0.3) >= 3
                THEN 'Moderate - prefer indoor restaurants'
            ELSE 'Bad weather - stay in or order delivery'
        END AS dining_recommendation,
        RANK() OVER (
            PARTITION BY ingestion_date
            ORDER BY (outdoor_comfort_score*0.40 + venue_density_score*0.30 + cuisine_diversity_score*0.30) DESC
        ) AS city_rank_today
    FROM combined
)

SELECT * FROM final
ORDER BY ingestion_date DESC, city_rank_today ASC
