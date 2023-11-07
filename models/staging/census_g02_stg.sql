{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'census') }}
),

transformed AS (
    SELECT
        CAST(lga_code_2016 AS VARCHAR) as lga_code,
        SUBSTR(CAST(lga_code_2016 AS VARCHAR), 4)::INT as clean_lga_code,
        COALESCE(median_age_persons, 0) as median_age_persons, -- Handling NULL values
        CAST(median_mortgage_repay_monthly AS INT) as median_mortgage_repay_monthly,
        CAST(median_tot_prsnl_inc_weekly AS INT) as median_tot_prsnl_inc_weekly,
        CAST(median_rent_weekly AS INT) as median_rent_weekly,
        CAST(median_tot_fam_inc_weekly AS INT) as median_tot_fam_inc_weekly,
        CAST(average_num_psns_per_bedroom AS INT) as average_num_psns_per_bedroom,
        CAST(median_tot_hhd_inc_weekly AS INT) as median_tot_hhd_inc_weekly,
        CAST(average_household_size AS INT) as average_household_size
    FROM source
)

SELECT * FROM transformed
