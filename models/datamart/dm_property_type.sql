-- models/dm_property_type.sql

{{ config(materialized='view') }}

WITH property_type_stats AS (
    SELECT
        tl.property_type,
        tl.room_type,
        tl.accommodates,
        TO_CHAR(tl.scraped_date, 'YYYY-MM') AS month_year,
        COUNT(CASE WHEN tl.has_availability = 't' THEN 1 END) AS total_active_listings,
        COUNT(CASE WHEN tl.has_availability = 'f' THEN 1 END) AS total_inactive_listings,
        COUNT(*) AS total_listings,
        MIN(CASE WHEN tl.has_availability = 't' THEN tl.price END) AS min_price,
        MAX(CASE WHEN tl.has_availability = 't' THEN tl.price END) AS max_price,
        
        -- Here we use percentile_cont for PostgreSQL to calculate the median
        percentile_cont(0.5) WITHIN GROUP (ORDER BY CASE WHEN tl.has_availability = 't' THEN tl.price END) AS median_price,
        
        AVG(CASE WHEN tl.has_availability = 't' THEN tl.price END) AS avg_price,
        COUNT(DISTINCT tl.original_host_id) AS distinct_hosts,
        COUNT(DISTINCT CASE WHEN tl.host_is_superhost = 't' THEN tl.original_host_id END) AS distinct_superhosts,
        AVG(CASE WHEN tl.has_availability = 't' THEN tl.review_scores_rating END) AS avg_review_scores_rating,
        SUM(CASE WHEN tl.has_availability = 't' THEN (30 - tl.availability_30) END) AS total_stays,
        AVG(CASE WHEN tl.has_availability = 't' THEN ((30 - tl.availability_30) * tl.price) END) AS avg_estimated_revenue_per_active_listing
    FROM {{ ref('facts_listings') }} fl
    INNER JOIN {{ ref('temp_listings_lga_suburb') }} tl ON fl.auto_gen_listing_id = tl.auto_gen_listing_id
    LEFT JOIN {{ ref('dim_lga') }} dl ON fl.lga_code = dl.lga_code
    LEFT JOIN {{ ref('dim_listings') }} dli ON fl.auto_gen_listing_id = dli.auto_gen_listing_id
    LEFT JOIN {{ ref('dim_host') }} dh ON fl.auto_gen_host_id = dh.auto_gen_host_id
    GROUP BY tl.property_type, tl.room_type, tl.accommodates, month_year
)

SELECT
    property_type,
    room_type,
    accommodates,
    month_year,
    CASE WHEN total_listings = 0 THEN NULL ELSE (total_active_listings / total_listings) * 100 END AS active_listings_rate,
    min_price,
    max_price,
    median_price,
    avg_price,
    distinct_hosts,
    CASE WHEN distinct_hosts = 0 THEN NULL ELSE (distinct_superhosts / distinct_hosts) * 100 END AS superhost_rate,
    avg_review_scores_rating,
    CASE WHEN LAG(total_active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year) = 0 THEN NULL 
         ELSE ((total_active_listings - LAG(total_active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year)) / 
               LAG(total_active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year)) * 100 
    END AS percent_change_active_listings,
    CASE WHEN LAG(total_inactive_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year) = 0 THEN NULL 
         ELSE ((total_inactive_listings - LAG(total_inactive_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year)) / 
               LAG(total_inactive_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year)) * 100 
    END AS percent_change_inactive_listings,
    total_stays,
    avg_estimated_revenue_per_active_listing
FROM property_type_stats
