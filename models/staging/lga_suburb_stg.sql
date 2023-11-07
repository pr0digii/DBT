
{{ config(materialized='view') }}

SELECT
  TRIM(lga_name) as lga_name, -- Removing leading and trailing whitespace
  INITCAP(TRIM(suburb_name)) as suburb_name -- Capitalizing the first letter of each word
FROM {{ source('raw','lga_suburb') }}
