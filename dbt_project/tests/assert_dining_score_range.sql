SELECT * FROM {{ ref('mart_city_dining_score') }}
WHERE dining_score < 0 OR dining_score > 10
