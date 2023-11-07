

{{ config(materialized='table') }}

SELECT
  *
FROM
  {{ ref('room_stg') }}
