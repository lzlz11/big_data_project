-- models/marts/mart_weather_impact_on_ratings.sql
-- FINAL OUTPUT: Weather Impact on Restaurant Ratings
--
-- Core question: Does weather affect restaurant ratings?
-- Analysis: Join daily weather conditions with average restaurant ratings per city
-- to see if good/bad weather correlates with higher/lower ratings



WITH weather AS (
    SELECT * FROM "airflow"."dbt_weather_restaurants"."stg_weather"
),

restaurants AS (
    SELECT * FROM "airflow"."dbt_weather_restaurants"."stg_restaurants"
    WHERE rating IS NOT NULL
),

-- Average restaurant ratings per city per day
avg_ratings AS (
    SELECT
        city_key,
        city_name,
        ingestion_date,
        ROUND(AVG(rating)::numeric, 2)          AS avg_rating,
        ROUND(AVG(popularity)::numeric, 3)       AS avg_popularity,
        COUNT(*)                                 AS total_rated_venues,
        COUNT(*) FILTER (WHERE rating >= 8.0)    AS excellent_venues,
        COUNT(*) FILTER (WHERE rating >= 6.0
                           AND rating < 8.0)     AS good_venues,
        COUNT(*) FILTER (WHERE rating < 6.0)     AS poor_venues
    FROM restaurants
    GROUP BY city_key, city_name, ingestion_date
),

-- Join weather + ratings
combined AS (
    SELECT
        w.city_key,
        w.city_name,
        w.ingestion_date,
        w.ingested_at_utc,

        -- Weather conditions
        w.temperature_c,
        w.humidity_pct,
        w.precipitation_mm,
        w.wind_speed_kmh,
        w.weather_description,
        w.weather_category,
        w.outdoor_comfort_score,

        -- Restaurant ratings
        r.avg_rating,
        r.avg_popularity,
        r.total_rated_venues,
        r.excellent_venues,
        r.good_venues,
        r.poor_venues,

        -- Weather impact label
        CASE
            WHEN w.weather_category = 'excellent'
                THEN 'Perfect outdoor weather'
            WHEN w.weather_category = 'good'
                THEN 'Pleasant weather'
            WHEN w.weather_category = 'moderate'
                THEN 'Cloudy / foggy'
            WHEN w.weather_category = 'bad'
                THEN 'Rainy / stormy'
            WHEN w.weather_category = 'very_bad'
                THEN 'Severe weather'
            ELSE 'Unknown'
        END                                      AS weather_label,

        -- Temperature comfort bucket
        CASE
            WHEN w.temperature_c < 0             THEN 'Freezing (<0°C)'
            WHEN w.temperature_c < 10            THEN 'Cold (0-10°C)'
            WHEN w.temperature_c < 20            THEN 'Cool (10-20°C)'
            WHEN w.temperature_c < 28            THEN 'Comfortable (20-28°C)'
            ELSE 'Hot (>28°C)'
        END                                      AS temp_bucket,

        -- Is it raining?
        CASE
            WHEN w.precipitation_mm > 1.0 THEN true
            ELSE false
        END                                      AS is_raining,

        -- Rating vs weather insight
        CASE
            WHEN w.outdoor_comfort_score >= 7
                 AND r.avg_rating >= 7.0
                THEN 'Good weather → High ratings'
            WHEN w.outdoor_comfort_score >= 7
                 AND r.avg_rating < 7.0
                THEN 'Good weather → Low ratings'
            WHEN w.outdoor_comfort_score < 5
                 AND r.avg_rating >= 7.0
                THEN 'Bad weather → High ratings'
            WHEN w.outdoor_comfort_score < 5
                 AND r.avg_rating < 7.0
                THEN 'Bad weather → Low ratings'
            ELSE 'Moderate conditions'
        END                                      AS weather_rating_insight

    FROM weather w
    LEFT JOIN avg_ratings r
        ON w.city_key = r.city_key
        AND w.ingestion_date = r.ingestion_date
)

SELECT * FROM combined
ORDER BY ingestion_date DESC, city_key