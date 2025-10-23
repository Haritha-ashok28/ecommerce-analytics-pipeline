{{ config(
    materialized = 'view',
    tags = ['staging']
) }}

SELECT 
    "product_id",
    LOWER(TRIM("product_category_name")) AS  product_category_name,
    CAST("product_name_lenght" AS INT) AS product_name_lenght,
    CAST("product_description_lenght" AS INT) AS product_description_lenght,
    CAST("product_photos_qty" AS INT) AS product_photos_qty,
    CAST("product_weight_g" AS FLOAT) AS product_weight_g,
    CAST("product_length_cm" AS FLOAT) AS product_length_cm,
    CAST("product_height_cm" AS FLOAT) AS product_height_cm,
    CAST("product_width_cm" AS FLOAT) AS product_width_cm
FROM {{ source('olist', 'olist_products_dataset') }}
