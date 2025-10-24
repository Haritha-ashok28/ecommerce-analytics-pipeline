-- models/intermediate/int_product_performance.sql
{{ config(
    materialized = 'view',
    tags = ['intermediate']
) }}

WITH product_sales AS (
    SELECT
        oi.product_id,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        SUM(oi.price) AS total_sales,
        AVG(oi.price) AS avg_price,
        SUM(oi.freight_value) AS total_freight,
        SUM(oi.price + oi.freight_value) AS gross_revenue
    FROM {{ ref('stg_order_items') }} oi
    JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
    WHERE o.order_status NOT IN ('canceled')
    GROUP BY oi.product_id
),

product_reviews AS (
    SELECT
        p.product_id,
        AVG(r.review_score) AS avg_review_score,
        COUNT(r.review_id) AS total_reviews
    FROM {{ ref('stg_order_items') }} p
    JOIN {{ ref('stg_reviews') }} r ON p.order_id = r.order_id
    GROUP BY p.product_id
),

product_details AS (
    SELECT
        product_id,
        product_category_name,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM {{ ref('stg_products') }}
)

SELECT
    ps.product_id,
    pd.product_category_name,
    ps.total_orders,
    ps.total_sales,
    ps.avg_price,
    ps.total_freight,
    ps.gross_revenue,
    pr.avg_review_score,
    pr.total_reviews,
    pd.product_weight_g,
    pd.product_length_cm,
    pd.product_height_cm,
    pd.product_width_cm
FROM product_sales ps
LEFT JOIN product_reviews pr ON ps.product_id = pr.product_id
LEFT JOIN product_details pd ON ps.product_id = pd.product_id
