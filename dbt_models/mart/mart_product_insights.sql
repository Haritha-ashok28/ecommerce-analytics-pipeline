-- this model aggregates product insights including total orders, units sold, revenue, and average price per product.
{{ config(
    materialized = 'table',
    tags = ['mart']
) }}

with base as (
    select
        o.order_id,
        o.order_purchase_ts::date as order_date,
        p.product_id,
        p.product_category_name,
        o.total_order_value as order_total_amount,
        o.order_item_id
    from {{ ref('int_order_summary') }} o
    left join {{ ref('int_product_performance') }} p
        on o.product_id = p.product_id
)

select
    product_id,
    product_category_name,
    count(distinct order_id) as total_orders,
    count(order_item_id) as total_units_sold,
    sum(order_total_amount) as total_revenue,
    avg(order_total_amount) as avg_price
from base
group by product_id, product_category_name
order by total_units_sold desc
