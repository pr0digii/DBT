-- models/staging/listings_stg.sql

{{ config(
    materialized='table',
    alias='listings'
) }}

WITH source_listings AS (
    SELECT
        listing_id,
        scrape_id,
        scraped_date,
        host_id,
        host_name,
        -- Checking if host_since is 'Na' or similar unexpected format, then replacing with NULL
        CASE
            WHEN host_since ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN host_since
            ELSE NULL
        END as host_since,
        host_is_superhost::char,
        host_neighbourhood,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates::integer,
        price,
        has_availability::char,
        availability_30::integer,
        number_of_reviews::integer,
        review_scores_rating::numeric,
        review_scores_accuracy::numeric,
        review_scores_cleanliness::numeric,
        review_scores_checkin::numeric,
        review_scores_communication::numeric,
        review_scores_value::numeric
    FROM {{ source('raw', 'listings') }}
),

transformed AS (
    SELECT
        *,
        -- Safely convert host_since to a date when it is not NULL
        CASE WHEN host_since IS NOT NULL THEN to_date(host_since, 'DD/MM/YYYY') END as host_since_date
    FROM source_listings
)

SELECT
    listing_id,
    scrape_id,
    scraped_date,
    host_id,
    host_name,
    -- If host_since_date is NULL, it will simply be NULL in the output as well
    host_since_date as host_since,
    host_is_superhost,
    host_neighbourhood,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    price,
    has_availability,
    availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value
FROM transformed
