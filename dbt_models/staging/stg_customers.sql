{{ config(
    materialized = 'view',
    tags = ['staging']
) }}

SELECT 
    "customer_id",
    LOWER(TRIM("customer_unique_id")) AS customer_uid,
    CAST("customer_zip_code_prefix" AS INT) AS customer_zip_prefix,
    LOWER(TRIM("customer_city")) AS customer_city,
    LOWER(TRIM("customer_state")) AS customer_state
FROM {{ source('olist', 'olist_customers_dataset') }}
