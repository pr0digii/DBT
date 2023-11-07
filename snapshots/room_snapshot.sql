
{% snapshot room_snapshot %}

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
    ROOM_TYPE,
    AVAILABILITY_30,
    NUMBER_OF_REVIEWS,
    REVIEW_SCORES_RATING,
    REVIEW_SCORES_ACCURACY,
    REVIEW_SCORES_CLEANLINESS,
    REVIEW_SCORES_CHECKIN,
    REVIEW_SCORES_COMMUNICATION,
    REVIEW_SCORES_VALUE
FROM {{ source('raw','listings') }}

{% endsnapshot %}
