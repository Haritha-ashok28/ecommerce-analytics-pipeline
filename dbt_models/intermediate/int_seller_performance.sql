-- models/intermediate/int_seller_performance.sql
-- Seller-level metrics

with order_items as (
    select * from {{ ref('stg_order_items') }}
),
orders as (
    select order_id, order_status, order_purchase_timestamp, order_delivered_customer_date
    from {{ ref('stg_orders') }}
),
sellers as (
    select seller_id, seller_city, seller_state from {{ ref('stg_sellers') }}
)

select
    s.seller_id,
    s.seller_city,
    s.seller_state,
    
    count(distinct oi.order_id) as total_orders,
    count(*) as total_items_sold,
    sum(oi.price + oi.freight_value) as total_revenue,
    
    avg(datediff(day, o.order_purchase_timestamp, o.order_delivered_customer_date)) as avg_delivery_days,
    
    sum(case when o.order_status = 'canceled' then 1 else 0 end) as canceled_orders

from order_items oi
join orders o on oi.order_id = o.order_id
join sellers s on oi.seller_id = s.seller_id
group by 1,2,3
