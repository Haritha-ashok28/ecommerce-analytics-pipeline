{{ config(
    materialized = 'view',
    tags = ['staging']
) }}

SELECT
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    CAST("shipping_limit_date" AS TIMESTAMP) AS shipping_limit_ts,
    CAST("price" AS FLOAT) AS price,
    CAST("freight_value" AS FLOAT) AS freight_value
FROM {{ source('olist', 'olist_order_items_dataset') }}
