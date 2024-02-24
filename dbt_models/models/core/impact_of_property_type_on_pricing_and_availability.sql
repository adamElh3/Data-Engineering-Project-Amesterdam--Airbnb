{{ config(materialized='table',
    schema='production')

 }}

SELECT
    Property_type,
    AVG(Price) AS avg_price,
    AVG(Availability_30) AS avg_availability_30,
    AVG(Availability_60) AS avg_availability_60,
    AVG(Availability_90) AS avg_availability_90
FROM
    {{ ref('basic_info') }} b
JOIN {{ref('availabilities')}} a
on b.Listing_id=a.Listing_id    
GROUP BY
    Property_type
ORDER BY
    avg_price DESC
