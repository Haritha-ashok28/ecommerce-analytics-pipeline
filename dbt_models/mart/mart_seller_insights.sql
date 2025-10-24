-- this model aggregates seller performance metrics for insights

{{ config(
    materialized = 'table',
    tags = ['mart']
) }}

with base as (
    select
        s.seller_id,
        o.order_id,
        o.total_order_value as order_total_amount,
        o.order_status,
        o.order_purchase_ts::date as order_date
    from {{ ref('int_seller_performance') }} s
    left join {{ ref('int_order_summary') }} o
        on s.seller_id = o.seller_id
)

select
    seller_id,
    count(distinct order_id) as total_orders,
    sum(order_total_amount) as total_revenue,
    avg(order_total_amount) as avg_order_value,
    sum(case when order_status = 'canceled' then 1 else 0 end) as cancellations,
    sum(case when order_status = 'canceled' then 1 else 0 end)/nullif(count(distinct order_id),0) as cancellation_ratio
from base
group by seller_id
order by total_revenue desc
