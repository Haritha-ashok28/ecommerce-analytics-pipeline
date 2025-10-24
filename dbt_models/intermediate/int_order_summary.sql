-- Purpose: Core Intermediate Model combining orders, order items, payments, and customers
-- Provides Order-Level metrics for downstream customer, seller, product, and payment models

{{ config(
    materialized = 'view',
    tags = ['intermediate']
) }}

WITH orders AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_ts,
        order_approved_ts,
        order_delivered_carrier_ts,
        order_delivered_customer_ts,
        order_estimated_delivery_ts
    FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        SUM(price) OVER (PARTITION BY order_id) AS total_price,
        SUM(freight_value) OVER (PARTITION BY order_id) AS total_freight,
        COUNT(DISTINCT product_id) OVER (PARTITION BY order_id) AS distinct_products,
        COUNT(order_item_id) OVER (PARTITION BY order_id) AS total_items
    FROM {{ ref('stg_order_items') }}
),

payments AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment_value,
        COUNT(DISTINCT payment_type) AS payment_methods_used,
        MAX(payment_installments) AS max_installments
    FROM {{ ref('stg_payments') }}
    GROUP BY order_id
),

customers AS (
    SELECT
        customer_id,
        customer_uid,
        customer_city,
        customer_state
    FROM {{ ref('stg_customers') }}
)


SELECT
    o.order_id,
    o.customer_id,
    c.customer_uid,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_ts,
    o.order_approved_ts,
    o.order_delivered_carrier_ts,
    o.order_delivered_customer_ts,
    o.order_estimated_delivery_ts,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    i.total_price,
    i.total_freight,
    i.distinct_products,
    i.total_items,
    p.total_payment_value,
    p.payment_methods_used,
    p.max_installments,
    (i.total_price + i.total_freight) AS total_order_value,
    DATEDIFF('day',o.order_purchase_ts, o.order_delivered_customer_ts) AS actual_delivery_days,
    DATEDIFF('day',o.order_purchase_ts, o.order_estimated_delivery_ts) AS estimated_delivery_days    
FROM orders o
LEFT JOIN order_items i ON o.order_id = i.order_id
LEFT JOIN payments p ON o.order_id = p.order_id
LEFT JOIN customers c ON o.customer_id = c.customer_id
