{{ config(materialized='view') }}

WITH cleaned_data AS (
  SELECT
    HOST_ID,
    HOST_NAME,
    -- Attempting to convert HOST_SINCE to a date with the format 'DD/MM/YYYY', setting NULL if it's an invalid format or not present
    CASE
      WHEN HOST_SINCE IS NOT NULL AND HOST_SINCE ~ '^\d{2}/\d{2}/\d{4}$' THEN
        TO_DATE(HOST_SINCE, 'DD/MM/YYYY')
      ELSE
        NULL
    END AS HOST_SINCE,
    CASE 
      WHEN HOST_IS_SUPERHOST = 't' THEN TRUE
      WHEN HOST_IS_SUPERHOST = 'f' THEN FALSE
      ELSE NULL
    END AS HOST_IS_SUPERHOST,
    HOST_NEIGHBOURHOOD
  FROM {{ ref('host_snapshot') }}
),

validated_data AS (
  SELECT
    HOST_ID,
    COALESCE(NULLIF(TRIM(HOST_NAME), ''), 'Unknown Host') AS HOST_NAME,
    HOST_SINCE,
    HOST_IS_SUPERHOST,
    COALESCE(NULLIF(TRIM(HOST_NEIGHBOURHOOD), ''), 'Unknown Neighbourhood') AS HOST_NEIGHBOURHOOD
  FROM cleaned_data
)

SELECT * FROM validated_data
