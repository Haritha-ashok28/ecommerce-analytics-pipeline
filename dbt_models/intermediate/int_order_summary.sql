-- Purpose: Core Intermediate Model combining orders, order items, payments, and customers
-- Provides Order-Level metrics for downstream customer, seller, product, and payment models

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
)
WITH order_items AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
)
WITH payments AS (
    SELECT
        order_id,
        payment_type,
        payment_installments,
        total_payment_value
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
)
WITH customers AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM {{ ref('stg_customers') }}
)

agg_order_item AS (
    SELECT
        oi.order_id,
        COUNT(DISTINCT oi.product_id) AS num_products,
        COUNT(*) AS num_items,
        SUM(oi.price) AS total_price,
        SUM(oi.freight_value) AS total_freight_value,
        SUM(oi.price + oi.freight_value) AS total_order_value
    FROM order_items oi
    GROUP BY oi.order_id
)
agg_payment AS (
    SELECT
        p.order_id,
        COUNT(p.payment_type) AS num_payment_methods,
        SUM(p.payment_value) AS total_paid_value,
        AVG(p.payment_value) AS avg_payment_value,
        MAX(p.payment_installments) AS max_installments
    FROM payments p
    GROUP BY p.order_id
)

SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_ts,
    o.order_approved_ts,
    o.order_delivered_carrier_ts,
    o.order_delivered_customer_ts,
    o.order_estimated_delivery_ts,
    aoi.num_products,
    aoi.num_items,
    aoi.total_price,
    aoi.total_freight_value,
    aoi.total_order_value,
    ap.num_payment_methods,
    ap.total_paid_value,
    ap.avg_payment_value,
    ap.max_installments,

    datediff(day, o.order_purchase_ts, o.order_approved_ts) AS days_to_approve,
    datediff(day, o.order_approved_ts, o.order_delivered_carrier_ts) AS days_to_ship,
    datediff(day, o.order_delivered_carrier_ts, o.order_delivered_customer_ts) AS days_to_deliver,
    datediff(day, o.order_purchase_ts, o.order_delivered_customer_ts) AS actual_delivery_days,
    datediff(day, o.order_purchase_ts, o.order_estimated_delivery_ts) AS estimated_delivery_days,
    case
        when o.order_status = 'canceled' then 1
        else 0
    end AS is_canceled
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN agg_order_item aoi ON o.order_id = aoi.order_id
LEFT JOIN agg_payment ap ON o.order_id = ap.order_id
