
{{ config(materialized='view') }}

SELECT
    ROOM_TYPE as room_type,
    AVAILABILITY_30 as availability_30,
    NUMBER_OF_REVIEWS as number_of_reviews,
    REVIEW_SCORES_RATING as review_scores_rating,
    REVIEW_SCORES_ACCURACY as review_scores_accuracy,
    REVIEW_SCORES_CLEANLINESS as review_scores_cleanliness,
    REVIEW_SCORES_CHECKIN as review_scores_checkin,
    REVIEW_SCORES_COMMUNICATION as review_scores_communication,
    REVIEW_SCORES_VALUE as review_scores_value
FROM
  {{ ref('room_snapshot') }}
