{{ config(materialized='table',
    schema='production')

 }}

SELECT Room_type, AVG(Price) AS avg_price
FROM {{ ref('basic_info') }}
GROUP BY Room_type
ORDER BY avg_price
