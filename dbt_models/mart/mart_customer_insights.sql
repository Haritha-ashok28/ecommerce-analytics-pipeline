-- This model creates a customer insights mart by aggregating customer behavior and order data.

{{ config(
    materialized = 'table',
    tags = ['mart']
) }}

with base as (
    select
        c.customer_id,
        c.customer_uid,
        o.order_id,
        o.order_purchase_ts::date as order_date,
        o.total_order_value as order_total_amount,
        c.customer_city,
        c.customer_state
    from {{ ref('int_customer_behaviour') }} c
    left join {{ ref('int_order_summary') }} o
        on c.customer_id = o.customer_id
),

customer_metrics as (
    select
        customer_id,
        count(distinct order_id) as total_orders,
        sum(order_total_amount) as lifetime_value,
        avg(order_total_amount) as avg_order_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(distinct order_id) / nullif(count(distinct customer_id),0) as repeat_rate,
        customer_city,
        customer_state
    from base
    group by customer_id, customer_city, customer_state
)

select *
from customer_metrics
