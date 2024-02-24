{{ config(materialized='table',
    schema='production')

 }}



SELECT Host_name, COUNT(Listing_id) AS listings_count
FROM {{ ref('basic_info') }}
GROUP BY Host_name
ORDER BY listings_count DESC
LIMIT 10
