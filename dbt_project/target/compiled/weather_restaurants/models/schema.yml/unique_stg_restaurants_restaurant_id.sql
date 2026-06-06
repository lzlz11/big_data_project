
    
    

select
    restaurant_id as unique_field,
    count(*) as n_records

from "airflow"."dbt_weather_restaurants"."stg_restaurants"
where restaurant_id is not null
group by restaurant_id
having count(*) > 1


