-- this model provides a daily sales overview including total orders, cancellations, revenue, and average order value
{{ config(
    materialized = 'table',
    tags = ['mart']
) }}

with base as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_ts::date as order_date,
        o.total_order_value as order_total_amount,
        COALESCE(p.payment_type, 'No Payment') as payment_type
    from {{ ref('int_order_summary') }} o
    left join {{ ref('int_payment_summary') }} p
        on o.order_id = p.order_id
)

select
    order_date,
    count(distinct order_id) as total_orders,
    sum(case when order_status = 'canceled' then 1 else 0 end) as total_cancellations,
    sum(order_total_amount) as total_revenue,
    avg(order_total_amount) as avg_order_value
from base
group by order_date
order by order_date
