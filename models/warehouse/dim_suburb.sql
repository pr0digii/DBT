-- dim_suburb.sql
{{ 
    config(
        materialized='table', 
        unique_key='suburb_id' 
    )
}}

WITH suburb_ranking AS (
    SELECT
        lga_code,
        suburb_name,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as suburb_id
    FROM {{ ref('lga_code_name_suburb_stg') }} 
    GROUP BY lga_code, suburb_name
)

SELECT
    suburb_id,
    lga_code,
    suburb_name
FROM suburb_ranking
