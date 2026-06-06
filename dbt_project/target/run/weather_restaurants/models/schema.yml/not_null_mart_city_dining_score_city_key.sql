select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select city_key
from "airflow"."dbt_weather_restaurants"."mart_city_dining_score"
where city_key is null



      
    ) dbt_internal_test