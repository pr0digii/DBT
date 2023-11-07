-- models/warehouse/dim_listings.sql

{{ 
    config(
        materialized='table',
        unique_key='auto_gen_listing_id' 
    )
}}

WITH base AS (
    SELECT
        listing_id AS original_listing_id,
        property_type,
        room_type,
        accommodates,
        has_availability
    FROM {{ ref('listings_stg') }} 
)

SELECT
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS auto_gen_listing_id,
    original_listing_id,
    property_type,
    room_type,
    accommodates,
    has_availability
FROM base
GROUP BY
    original_listing_id,
    property_type,
    room_type,
    accommodates,
    has_availability
