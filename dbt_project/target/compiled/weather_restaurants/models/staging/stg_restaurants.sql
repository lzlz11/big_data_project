-- Staging: normalize OpenStreetMap restaurant data



SELECT
    osm_id::bigint                                      AS restaurant_id,
    city_key,
    city_name,
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown')         AS restaurant_name,
    CASE amenity
        WHEN 'restaurant' THEN 'restaurant'
        WHEN 'cafe'       THEN 'cafe'
        WHEN 'fast_food'  THEN 'fast_food'
        ELSE 'other'
    END                                                 AS place_type,
    COALESCE(NULLIF(TRIM(cuisine), ''), 'unknown')      AS cuisine,
    ROUND(latitude::numeric, 6)                         AS latitude,
    ROUND(longitude::numeric, 6)                        AS longitude,
    opening_hours,
    (ingested_at AT TIME ZONE 'UTC')::date              AS ingestion_date,
    (ingested_at AT TIME ZONE 'UTC')::timestamp         AS ingested_at_utc
FROM "airflow"."dbt_weather_restaurants"."restaurants_raw"
WHERE osm_id IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL