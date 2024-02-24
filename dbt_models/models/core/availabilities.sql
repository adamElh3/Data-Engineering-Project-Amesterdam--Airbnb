SELECT
    Listing_id, 
    Availability_30,
    Availability_60,
    Availability_90

FROM {{ source('staging', 'listings') }}