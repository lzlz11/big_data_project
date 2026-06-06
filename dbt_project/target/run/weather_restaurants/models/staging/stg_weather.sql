
  create view "airflow"."dbt_weather_restaurants"."stg_weather__dbt_tmp"
    
    
  as (
    -- models/staging/stg_weather.sql
-- Normalize Open-Meteo weather data



WITH raw_data AS (
    SELECT * FROM "airflow"."dbt_weather_restaurants"."weather_raw"
)

SELECT
    city_key,
    city_name,
    (ingested_at AT TIME ZONE 'UTC')::date              AS ingestion_date,
    (ingested_at AT TIME ZONE 'UTC')::timestamp         AS ingested_at_utc,
    ROUND(current_temperature::numeric, 1)              AS temperature_c,
    ROUND(current_humidity::numeric, 0)                 AS humidity_pct,
    ROUND(current_precipitation::numeric, 2)            AS precipitation_mm,
    ROUND(current_wind_speed::numeric, 1)               AS wind_speed_kmh,
    current_weather_code::integer                       AS weather_code,
    CASE
        WHEN current_weather_code::int = 0              THEN 'Clear sky'
        WHEN current_weather_code::int IN (1,2,3)       THEN 'Partly cloudy'
        WHEN current_weather_code::int IN (45,48)       THEN 'Foggy'
        WHEN current_weather_code::int BETWEEN 51 AND 67 THEN 'Rainy'
        WHEN current_weather_code::int BETWEEN 71 AND 77 THEN 'Snowy'
        WHEN current_weather_code::int BETWEEN 80 AND 82 THEN 'Rain showers'
        WHEN current_weather_code::int BETWEEN 95 AND 99 THEN 'Thunderstorm'
        ELSE 'Other'
    END                                                 AS weather_description,
    CASE
        WHEN current_weather_code::int = 0              THEN 'excellent'
        WHEN current_weather_code::int IN (1,2,3)       THEN 'good'
        WHEN current_weather_code::int IN (45,48)       THEN 'moderate'
        WHEN current_weather_code::int BETWEEN 51 AND 82 THEN 'bad'
        WHEN current_weather_code::int BETWEEN 95 AND 99 THEN 'very_bad'
        ELSE 'unknown'
    END                                                 AS weather_category,
    CASE
        WHEN current_weather_code::int = 0
             AND current_temperature::numeric BETWEEN 15 AND 28
             AND current_wind_speed::numeric < 20       THEN 10
        WHEN current_weather_code::int IN (1,2,3)
             AND current_temperature::numeric BETWEEN 10 AND 30 THEN 7
        WHEN current_weather_code::int IN (45,48)       THEN 4
        WHEN current_weather_code::int BETWEEN 51 AND 82 THEN 2
        WHEN current_weather_code::int BETWEEN 95 AND 99 THEN 0
        ELSE 5
    END                                                 AS outdoor_comfort_score
FROM raw_data
WHERE city_key IS NOT NULL
  );