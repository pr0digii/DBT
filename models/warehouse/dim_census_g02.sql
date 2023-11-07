{{
    config(
        unique_key='lga_code_2016'
    )
}}

select * from {{ ref('census_g02_stg') }}