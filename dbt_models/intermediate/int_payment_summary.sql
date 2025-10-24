-- models/intermediate/int_payment_summary.sql
-- Payment-type metrics

{{ config(
    materialized = 'view',
    tags = ['intermediate']
) }}

SELECT
    p.payment_type,
    p.order_id, 
    COUNT(*) OVER (PARTITION BY p.payment_type) AS total_payments,
    SUM(p.payment_value) OVER (PARTITION BY p.payment_type) AS total_payment_value,
    AVG(p.payment_value) OVER (PARTITION BY p.payment_type) AS avg_payment_value,
    AVG(p.payment_installments) OVER (PARTITION BY p.payment_type) AS avg_installments
FROM {{ ref('stg_payments') }} p
JOIN {{ ref('stg_orders') }} o 
  ON p.order_id = o.order_id
WHERE o.order_status NOT IN ('canceled')
ORDER BY total_payment_value DESC
