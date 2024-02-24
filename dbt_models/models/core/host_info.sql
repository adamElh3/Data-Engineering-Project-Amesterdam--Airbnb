SELECT    
    Host_id, 
    Host_name, 
    Host_response_time,
    Host_response_rate,
    Superhost_status,
    Host_listings_count,
    Host_total_listings_count

FROM {{ source('staging', 'listings') }}   
