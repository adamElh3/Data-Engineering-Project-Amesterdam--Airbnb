SELECT
    Listing_id, 
    Listing_name, 
    Host_id, 
    Host_name, 
    Neighborhood, 
    Latitude, 
    Longitude, 
    Room_type ,
    Price ,
    Min_nights,
    Property_type

FROM {{ source('staging', 'listings') }}
