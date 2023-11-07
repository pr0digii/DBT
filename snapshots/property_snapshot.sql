

{% snapshot property_snapshot %}

{{ 
    config(
        target_schema='raw',
        strategy='timestamp',
        unique_key='listing_id',
        updated_at='SCRAPED_DATE',
    ) 
}}

SELECT
     listing_id,
    scraped_date,
    PROPERTY_TYPE,
    ACCOMMODATES,
    PRICE
FROM {{ source('raw','listings') }}

{% endsnapshot %}
