-- models/intermediate/int_seller_performance.sql
-- Seller-level metrics

{{ config(
    materialized = 'view',
    tags = ['intermediate']
) }}

WITH seller_orders AS (
    SELECT
        s.seller_id,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        SUM(oi.price) AS total_sales,
        SUM(oi.freight_value) AS total_freight,
        SUM(oi.price + oi.freight_value) AS gross_revenue
    FROM {{ ref('stg_order_items') }} oi
    JOIN {{ ref('stg_sellers') }} s ON oi.seller_id = s.seller_id
    JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
    WHERE o.order_status NOT IN ('canceled')
    GROUP BY s.seller_id
),

seller_reviews AS (
    SELECT
        oi.seller_id,
        AVG(r.review_score) AS avg_review_score,
        COUNT(r.review_id) AS total_reviews
    FROM {{ ref('stg_order_items') }} oi
    JOIN {{ ref('stg_reviews') }} r ON oi.order_id = r.order_id
    GROUP BY oi.seller_id
)

SELECT
    so.seller_id,
    so.total_orders,
    so.total_sales,
    so.total_freight,
    so.gross_revenue,
    sr.avg_review_score,
    sr.total_reviews
FROM seller_orders so
LEFT JOIN seller_reviews sr ON so.seller_id = sr.seller_id
