select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT ingestion_date, COUNT(DISTINCT city_key) AS city_count
FROM "airflow"."dbt_weather_restaurants"."mart_city_dining_score"
GROUP BY ingestion_date
HAVING COUNT(DISTINCT city_key) < 4
      
    ) dbt_internal_test