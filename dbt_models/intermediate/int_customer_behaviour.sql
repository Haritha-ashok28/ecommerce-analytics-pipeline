-- models/intermediate/int_customer_behavior.sql
-- Aggregates order-level metrics to customer-level

with order_summary as (
    select * from {{ ref('int_order_summary') }}
)

select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    
    -- Customer order metrics
    count(distinct order_id) as total_orders,
    sum(total_order_value) as total_spend,
    sum(total_payment_value) as total_paid,
    avg(total_order_value) as avg_order_value,
    
    -- Order status metrics
    sum(is_canceled) as total_canceled_orders,
    round(sum(is_canceled)::numeric / nullif(count(*),0),2) as canceled_ratio,
    
    -- Repeat purchase metrics
    count(distinct order_id) filter (where is_canceled = 0) as completed_orders,
    round(sum(total_order_value) filter (where is_canceled = 0) / nullif(count(distinct order_id) filter (where is_canceled = 0),0),2) as avg_completed_order_value

from order_summary
group by 1,2,3,4
