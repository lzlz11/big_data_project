select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT * FROM "airflow"."dbt_weather_restaurants"."mart_city_dining_score"
WHERE dining_score < 0 OR dining_score > 10
      
    ) dbt_internal_test