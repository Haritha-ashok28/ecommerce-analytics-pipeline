{{ config(
    materialized = 'view',
    tags = ['staging']
) }}

SELECT
    "seller_id",
    CAST("seller_zip_code_prefix" AS INT) AS seller_zip_prefix,
    LOWER(TRIM("seller_city")) AS seller_city,
    LOWER(TRIM("seller_state")) AS seller_state
FROM {{ source('olist', 'olist_sellers_dataset') }}
