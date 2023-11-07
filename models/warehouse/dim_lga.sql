-- models/dim_lga.sql

{{ config(
    materialized='table',
    alias='dim_lga'
) }}

with lga_code as (
    select * from {{ ref('lga_code_name_stg') }}
),

go1_census as (
    select * from {{ ref('census_g01_stg') }}
),

go2_census as (
    select * from {{ ref('census_g02_stg') }}
)

select 
    lga.lga_code,
    lga.lga_name,
    go1.tot_p_m,
    go1.tot_p_f,
    go1.tot_p_p,
    go1.Age_0_4_yr_M,
    go1.Age_0_4_yr_F,
    go1.Age_0_4_yr_P,
    go1.Age_5_14_yr_M,
    go1.Age_5_14_yr_F,
    go1.Age_5_14_yr_P,
    go1.Age_15_19_yr_M,
    go1.Age_15_19_yr_F,
    go1.Age_15_19_yr_P,
    go1.Age_20_24_yr_M,
    go1.Age_20_24_yr_F,
    go1.Age_20_24_yr_P,
    go1.Age_25_34_yr_M,
    go1.Age_25_34_yr_F,
    go1.Age_25_34_yr_P,
    go2.median_age_persons,
    go2.median_mortgage_repay_monthly,
    go2.median_tot_prsnl_inc_weekly,
    go2.median_rent_weekly,
    go2.median_tot_fam_inc_weekly,
    go2.average_num_psns_per_bedroom,
    go2.median_tot_hhd_inc_weekly,
    go2.average_household_size
from lga_code lga
left join go1_census go1 on lga.lga_code = go1.clean_lga_code
left join go2_census go2 on lga.lga_code = go2.clean_lga_code
