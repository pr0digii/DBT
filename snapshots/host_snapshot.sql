
{% snapshot host_snapshot %}

{{ 
    config(
        target_schema='raw',
        strategy='timestamp',
        unique_key='host_id',
        updated_at='SCRAPED_DATE',
        
    ) 
}}

SELECT
    
    HOST_ID,
    SCRAPED_DATE,
    HOST_NAME,
    HOST_SINCE,
    HOST_IS_SUPERHOST,
    HOST_NEIGHBOURHOOD
FROM {{ source('raw','listings') }}

{% endsnapshot %}
