-- dm_host_neighbourhood.sql in the /models directory
{{ config(materialized='view') }}

WITH host_neighbourhood_stats AS (
    SELECT
        d_lga.lga_name AS host_neighbourhood_lga,
        TO_CHAR(tl.scraped_date, 'YYYY-MM') AS month_year,
        COUNT(DISTINCT tl.original_host_id) AS distinct_hosts,
        SUM((30 - tl.availability_30) * tl.price) AS estimated_revenue,
        -- Adjusting the CASE statement for string to boolean conversion
        SUM(CASE WHEN tl.has_availability = 't' THEN ((30 - tl.availability_30) * tl.price) ELSE 0 END) AS estimated_revenue_active_listing
    FROM {{ ref('facts_listings') }} AS fl
    LEFT JOIN {{ ref('dim_lga') }} AS d_lga ON fl.lga_code = d_lga.lga_code
    LEFT JOIN {{ ref('temp_listings_lga_suburb') }} AS tl ON fl.auto_gen_listing_id = tl.auto_gen_listing_id
    LEFT JOIN {{ ref('dim_host') }} AS d_host ON fl.auto_gen_host_id = d_host.auto_gen_host_id
    LEFT JOIN {{ ref('dim_suburb') }} AS d_suburb ON fl.suburb_id = d_suburb.suburb_id
    GROUP BY d_lga.lga_name, month_year
)

SELECT
    host_neighbourhood_lga,
    month_year,
    distinct_hosts,
    estimated_revenue,
    CASE WHEN distinct_hosts = 0 THEN NULL ELSE estimated_revenue_active_listing / distinct_hosts END AS estimated_revenue_per_host
FROM host_neighbourhood_stats
