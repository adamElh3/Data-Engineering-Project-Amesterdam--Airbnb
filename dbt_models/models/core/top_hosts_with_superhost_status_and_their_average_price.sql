{{ config(materialized='table',
    schema='production')

 }}


SELECT
    b.Host_name,
    AVG(b.Price) AS avg_price
FROM
   {{ ref('basic_info') }} b
JOIN
    {{ ref('host_info') }} a ON b.Host_id = a.Host_id
WHERE
    a.Superhost_status = true
GROUP BY
    b.Host_name
ORDER BY
    avg_price DESC
