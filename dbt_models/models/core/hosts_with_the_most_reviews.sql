{{ config(materialized='table',
    schema='production')

 }}

SELECT
    h.Host_name,
    SUM(r.num_reviews) AS num_reviews
FROM
    {{ ref('basic_info') }} h
JOIN
    {{ ref('review_info') }} r ON h.Host_id = r.Host_id
GROUP BY
    h.Host_name
ORDER BY
    num_reviews DESC
LIMIT 10
