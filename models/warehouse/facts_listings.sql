-- fact_listings.sql in the /models directory
{{ config(materialized='table') }}

SELECT
    auto_gen_listing_id, 
    auto_gen_host_id, 
    lga_code, 
    suburb_id,
    date_id, 
    price, 
    availability_30,
    number_of_reviews, 
    review_scores_rating
FROM {{ ref('temp_listings_lga_suburb') }}
