-- models/dm_listing_neighbourhood.sql

{{ config(materialized='view') }}

WITH listing_neighbourhood_stats AS (
    SELECT
        tl.listing_neighbourhood,
        TO_CHAR(tl.scraped_date, 'YYYY-MM') AS month_year,
        COUNT(CASE WHEN tl.has_availability = 't' THEN 1 END) AS total_active_listings,
        COUNT(CASE WHEN tl.has_availability = 'f' THEN 1 END) AS total_inactive_listings,
        COUNT(*) AS total_listings,
        MIN(CASE WHEN tl.has_availability = 't' THEN tl.price END) AS min_price,
        MAX(CASE WHEN tl.has_availability = 't' THEN tl.price END) AS max_price,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY tl.price) FILTER (WHERE tl.has_availability = 't') AS median_price,
        AVG(CASE WHEN tl.has_availability = 't' THEN tl.price END) AS avg_price,
        COUNT(DISTINCT tl.auto_gen_host_id) AS distinct_hosts,
        COUNT(DISTINCT CASE WHEN tl.host_is_superhost = 't' THEN tl.auto_gen_host_id END) AS distinct_superhosts,
        AVG(CASE WHEN tl.has_availability = 't' THEN tl.review_scores_rating END) AS avg_review_scores_rating,
        SUM(CASE WHEN tl.has_availability = 't' THEN (30 - tl.availability_30) END) AS total_stays,
        AVG(CASE WHEN tl.has_availability = 't' THEN (30 - tl.availability_30) * tl.price END) AS avg_estimated_revenue_per_active_listing
    FROM {{ ref('facts_listings') }} fl
    INNER JOIN {{ ref('temp_listings_lga_suburb') }} tl ON fl.auto_gen_listing_id = tl.auto_gen_listing_id
    GROUP BY tl.listing_neighbourhood, month_year
)

SELECT
    listing_neighbourhood,
    month_year,
    total_active_listings,
    total_inactive_listings,
    total_listings,
    active_listings_rate,
    min_price,
    max_price,
    median_price,
    avg_price,
    distinct_hosts,
    distinct_superhosts,
    superhost_rate,
    avg_review_scores_rating,
    percent_change_active_listings,
    percent_change_inactive_listings,
    total_stays,
    avg_estimated_revenue_per_active_listing
FROM (
    SELECT
        *,
        CASE WHEN total_listings = 0 THEN NULL ELSE (total_active_listings * 100.0 / total_listings) END AS active_listings_rate,
        CASE WHEN LAG(total_active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) IS NULL THEN NULL ELSE 
            (total_active_listings - LAG(total_active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year)) * 100.0 / NULLIF(LAG(total_active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year), 0) 
            END AS percent_change_active_listings,
        CASE WHEN LAG(total_inactive_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) IS NULL THEN NULL ELSE 
            (total_inactive_listings - LAG(total_inactive_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year)) * 100.0 / NULLIF(LAG(total_inactive_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year), 0) 
            END AS percent_change_inactive_listings,
        CASE WHEN distinct_hosts = 0 THEN NULL ELSE (distinct_superhosts * 100.0 / distinct_hosts) END AS superhost_rate
    FROM listing_neighbourhood_stats
) AS final
