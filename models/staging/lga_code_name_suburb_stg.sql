{{ config(materialized='view') }}

WITH lga_codes AS (
  SELECT
    lga_code,
    TRIM(lga_name) as lga_name_cleaned -- Removing leading and trailing whitespace
  FROM
    {{ source('raw', 'lga_code_name') }}
),

lga_suburbs AS (
  SELECT
    TRIM(lga_name) as lga_name_cleaned, -- Removing leading and trailing whitespace
    INITCAP(TRIM(suburb_name)) as suburb_name -- Capitalizing the first letter of each word
  FROM
    {{ source('raw','lga_suburb') }}
)

SELECT
  c.lga_code,
  c.lga_name_cleaned AS lga_name,
  s.suburb_name
FROM lga_codes AS c
LEFT JOIN lga_suburbs AS s
  ON lower(c.lga_name_cleaned) = lower(s.lga_name_cleaned)
