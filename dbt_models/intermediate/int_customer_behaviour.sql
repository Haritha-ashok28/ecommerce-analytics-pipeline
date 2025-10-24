-- models/intermediate/int_customer_behavior.sql
-- Aggregates order-level metrics to customer-level

{{ config(
    materialized = 'view',
    tags = ['intermediate']
) }}

WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_order_value) AS total_spent,
        AVG(total_order_value) AS avg_order_value,
        MAX(order_purchase_ts) AS last_order_date,
        MIN(order_purchase_ts) AS first_order_date
    FROM {{ ref('int_order_summary') }}
    WHERE order_status NOT IN ('canceled')
    GROUP BY customer_id
),

order_frequency AS (
    SELECT
        customer_id,
        DATEDIFF('day', MIN(order_purchase_ts), MAX(order_purchase_ts)) 
            / NULLIF(COUNT(DISTINCT order_id) - 1, 0) AS avg_days_between_orders
    FROM {{ ref('int_order_summary') }}
    GROUP BY customer_id
),

customer_location AS (
    SELECT
        customer_id,
        customer_uid,
        customer_city,
        customer_state
    FROM {{ ref('stg_customers') }}
)

SELECT
    co.customer_id,
    cl.customer_uid,
    cl.customer_city,
    cl.customer_state,
    co.total_orders,
    co.total_spent,
    co.avg_order_value,
    co.first_order_date,
    co.last_order_date,
    ofreq.avg_days_between_orders,
    DATEDIFF('day', co.last_order_date, CURRENT_DATE()) AS days_since_last_order
FROM customer_orders co
LEFT JOIN order_frequency ofreq ON co.customer_id = ofreq.customer_id
LEFT JOIN customer_location cl ON co.customer_id = cl.customer_id

