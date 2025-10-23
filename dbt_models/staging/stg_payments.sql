{{ config(
    materialized = 'view',
    tags = ['staging']
) }}

SELECT 
    "order_id",
    CAST("payment_sequential" AS INT) AS payment_sequential,
    LOWER(TRIM("payment_type")) AS payment_type,
    CAST("payment_installments" AS INT) AS payment_installments,
    CAST("payment_value" AS FLOAT) AS payment_value
FROM {{ source('olist', 'olist_order_payments_dataset') }}
