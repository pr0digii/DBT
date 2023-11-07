

{{ config(materialized='view') }}

SELECT
    PROPERTY_TYPE,
    ACCOMMODATES,
    PRICE
FROM {{ ref('property_snapshot') }}
