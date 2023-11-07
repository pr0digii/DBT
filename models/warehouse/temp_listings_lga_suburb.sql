-- temp_listings_lga_suburb.sql in the /models directory
{{ config(materialized='table') }}

WITH listings_enriched AS (
    SELECT
        dl.auto_gen_listing_id,
        dl.original_listing_id,
        l.scrape_id,
        l.scraped_date,
        dh.auto_gen_host_id,
        dh.original_host_id,
        l.host_name,
        l.host_since,
        l.host_is_superhost,
        l.host_neighbourhood,
        l.listing_neighbourhood,
        l.property_type,
        l.room_type,
        l.accommodates,
        l.price,
        l.has_availability,
        l.availability_30,
        l.number_of_reviews,
        l.review_scores_rating,
        l.review_scores_accuracy,
        l.review_scores_cleanliness,
        l.review_scores_checkin,
        l.review_scores_communication,
        l.review_scores_value,
        n.lga_code,
        n.lga_name,
        ds.suburb_id,
        ds.suburb_name,
        dd.date_id,
        dd.month_year_date -- Using the date column from dim_date
    FROM {{ ref('listings_stg') }} l
    LEFT JOIN {{ ref('lga_code_name_stg') }} n ON l.listing_neighbourhood = n.lga_name
    LEFT JOIN {{ ref('dim_suburb') }} ds ON LOWER(l.host_neighbourhood) = LOWER(ds.suburb_name)
    LEFT JOIN {{ ref('dim_date') }} dd ON l.scraped_date = dd.date
    LEFT JOIN {{ ref('dim_listings') }} dl ON l.listing_id = dl.original_listing_id
        AND l.property_type = dl.property_type
        AND l.room_type = dl.room_type
        AND l.accommodates = dl.accommodates
        AND l.has_availability = dl.has_availability
    LEFT JOIN {{ ref('dim_host') }} dh ON l.host_id = dh.original_host_id
        AND l.host_name = dh.host_name
        AND l.host_is_superhost = dh.host_is_superhost
        AND l.host_since = dh.host_since
)

SELECT * FROM listings_enriched
