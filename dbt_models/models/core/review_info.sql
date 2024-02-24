
SELECT
    Listing_id,
    Num_reviews,
    cast(last_review as timestamp) as last_review,
    Reviews_per_month,
    Review_scores_rating,
    Host_id
    FROM {{ source('staging', 'listings') }}
