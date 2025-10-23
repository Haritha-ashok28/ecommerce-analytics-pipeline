{{ config(
    materialized = 'view',
    tags = ['staging']
) }}

SELECT
    "review_id",
    "order_id",
    CAST("review_score" AS INT) AS review_score,
    LOWER(TRIM("review_comment_title")) AS review_title,
    LOWER(TRIM("review_comment_message")) AS review_message,
    CAST("review_creation_date" AS TIMESTAMP) AS review_creation_ts,
    CAST("review_answer_timestamp" AS TIMESTAMP) AS review_answered_ts
FROM {{ source('olist', 'olist_order_reviews_dataset') }}
