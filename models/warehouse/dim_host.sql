-- dim_host.sql
{{ 
    config(
        materialized='table',
        unique_key='auto_gen_host_id' 
    )
}}

WITH host_data AS (
    SELECT
        host_id AS original_host_id,
        host_name,
        host_is_superhost,
        host_since,
        COUNT(*) OVER (PARTITION BY host_id) AS host_count
    FROM {{ ref('listings_stg') }} 
)

SELECT
    ROW_NUMBER() OVER (ORDER BY host_count DESC) AS auto_gen_host_id,
    original_host_id,
    host_name,
    host_is_superhost,
    host_since
FROM host_data
GROUP BY
    original_host_id,
    host_name,
    host_is_superhost,
    host_since,
    host_count
