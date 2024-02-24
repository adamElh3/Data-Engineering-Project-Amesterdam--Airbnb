{{ config(materialized='table',
    schema='production')

 }}

SELECT
    Property_type,
    COUNT(*) AS property_count
FROM
    {{ ref('basic_info') }}
GROUP BY
    Property_type
ORDER BY
    property_count DESC
