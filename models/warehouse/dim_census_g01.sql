

{{ config(materialized='table') }}

SELECT
  *
FROM
  {{ ref('census_g01_stg') }}
