-- lga_stg.sql

{{ config(materialized='view') }}

SELECT
  lga_code,
  TRIM(lga_name) as lga_name -- Removing leading and trailing whitespace
FROM
  {{ source('raw', 'lga_code_name') }}
