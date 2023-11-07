

{{ config(materialized='table') }}

SELECT
  *
FROM
  {{ ref('property_stg') }}
