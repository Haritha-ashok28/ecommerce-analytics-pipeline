-- This mart provides insights into payment methods and customer locations
{{ config(
    materialized = 'table',
    tags = ['mart']
) }}

with base as (
    select
        o.order_id,
        o.order_purchase_ts::date as order_date,
        o.total_order_value as order_total_amount,
        p.payment_type,
        c.customer_city,
        c.customer_state
    from {{ ref('int_order_summary') }} o
    left join {{ ref('int_payment_summary') }} p
        on o.order_id = p.order_id
    left join {{ ref('int_customer_behaviour') }} c
        on o.customer_id = c.customer_id
)

select
    payment_type,
    customer_state,
    count(distinct order_id) as total_orders,
    sum(order_total_amount) as total_revenue,
    avg(order_total_amount) as avg_order_value
from base
group by payment_type, customer_state
order by total_revenue desc
