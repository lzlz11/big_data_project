-- Staging: normalize Open-Meteo air quality data



WITH raw_data AS (
    SELECT * FROM "airflow"."dbt_weather_restaurants"."air_quality_raw"
)

SELECT
    city_key,
    city_name,
    (ingested_at AT TIME ZONE 'UTC')::date              AS ingestion_date,
    (ingested_at AT TIME ZONE 'UTC')::timestamp         AS ingested_at_utc,
    selected_hour_utc,
    ROUND(current_pm10::numeric, 1)                     AS pm10,
    ROUND(current_pm2_5::numeric, 1)                    AS pm2_5,
    ROUND(current_carbon_monoxide::numeric, 1)          AS carbon_monoxide,
    ROUND(current_nitrogen_dioxide::numeric, 1)         AS nitrogen_dioxide,
    ROUND(current_sulphur_dioxide::numeric, 1)          AS sulphur_dioxide,
    ROUND(current_ozone::numeric, 1)                    AS ozone,
    ROUND(current_dust::numeric, 1)                     AS dust,
    ROUND(current_uv_index::numeric, 1)                 AS uv_index,
    ROUND(current_us_aqi::numeric, 0)                   AS us_aqi,
    ROUND(current_european_aqi::numeric, 0)             AS european_aqi,
    CASE
        WHEN current_european_aqi IS NOT NULL AND current_european_aqi <= 20 THEN 'good'
        WHEN current_european_aqi IS NOT NULL AND current_european_aqi <= 40 THEN 'fair'
        WHEN current_european_aqi IS NOT NULL AND current_european_aqi <= 60 THEN 'moderate'
        WHEN current_european_aqi IS NOT NULL AND current_european_aqi <= 80 THEN 'poor'
        WHEN current_european_aqi IS NOT NULL AND current_european_aqi <= 100 THEN 'very_poor'
        WHEN current_european_aqi IS NOT NULL THEN 'extremely_poor'
        WHEN current_us_aqi IS NOT NULL AND current_us_aqi <= 50 THEN 'good'
        WHEN current_us_aqi IS NOT NULL AND current_us_aqi <= 100 THEN 'moderate'
        WHEN current_us_aqi IS NOT NULL AND current_us_aqi <= 150 THEN 'unhealthy_sensitive'
        WHEN current_us_aqi IS NOT NULL AND current_us_aqi <= 200 THEN 'unhealthy'
        WHEN current_us_aqi IS NOT NULL AND current_us_aqi <= 300 THEN 'very_unhealthy'
        WHEN current_us_aqi IS NOT NULL THEN 'hazardous'
        ELSE 'unknown'
    END                                                 AS air_quality_category,
    CASE
        WHEN current_european_aqi IS NOT NULL AND current_european_aqi <= 20 THEN 10
        WHEN current_european_aqi IS NOT NULL AND current_european_aqi <= 40 THEN 8
        WHEN current_european_aqi IS NOT NULL AND current_european_aqi <= 60 THEN 5
        WHEN current_european_aqi IS NOT NULL AND current_european_aqi <= 80 THEN 2
        WHEN current_european_aqi IS NOT NULL THEN 0
        WHEN current_us_aqi IS NOT NULL AND current_us_aqi <= 50 THEN 10
        WHEN current_us_aqi IS NOT NULL AND current_us_aqi <= 100 THEN 8
        WHEN current_us_aqi IS NOT NULL AND current_us_aqi <= 150 THEN 5
        WHEN current_us_aqi IS NOT NULL AND current_us_aqi <= 200 THEN 2
        WHEN current_us_aqi IS NOT NULL THEN 0
        ELSE 5
    END                                                 AS air_quality_score
FROM raw_data
WHERE city_key IS NOT NULL