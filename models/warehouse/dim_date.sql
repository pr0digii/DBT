-- models/warehouse/dim_date.sql

{{ 
    config(
        materialized='table',
        unique_key='date_id' 
    )
}}

WITH date_range AS (
    SELECT
        generate_series(
            (SELECT MIN(scraped_date) FROM {{ ref('listings_stg') }}),
            (SELECT MAX(scraped_date) FROM {{ ref('listings_stg') }}),
            interval '1 day'
        ) AS date
),

dim_date_with_month_year_date AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY date) AS date_id,
        date,
        to_date('01' || '_' || to_char(date, 'MM_YYYY'), 'DD_MM_YYYY')::date AS month_year_date
    FROM date_range
)

SELECT * FROM dim_date_with_month_year_date
