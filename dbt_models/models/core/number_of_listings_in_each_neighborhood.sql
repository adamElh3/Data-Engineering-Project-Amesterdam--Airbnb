{{ config(materialized='table' 
,schema = 'production')}}


SELECT
    Neighborhood,
    AVG(Price) AS avg_price,
    AVG(Latitude) AS avg_latitude,
    AVG(Longitude) AS avg_longitude,
    COUNT(*) AS listing_count
FROM
    {{ ref('basic_info') }}
GROUP BY
    Neighborhood
ORDER BY
    avg_price DESC    




