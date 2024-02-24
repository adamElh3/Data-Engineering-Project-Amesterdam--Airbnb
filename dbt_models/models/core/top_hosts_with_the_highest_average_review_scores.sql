{{ config(materialized='table',
    schema='production')
}}



with HostReviewScores AS (
    SELECT
        Host_id,
        AVG(Review_scores_rating) AS avg_review_score
    FROM
        {{ ref('review_info') }}
    GROUP BY
        Host_id
)



SELECT
    h.Host_name,
    hrs.avg_review_score
FROM
    HostReviewScores hrs
JOIN
    {{ ref('basic_info') }} h ON hrs.host_id = h.host_id
ORDER BY
    hrs.avg_review_score DESC
LIMIT 10

