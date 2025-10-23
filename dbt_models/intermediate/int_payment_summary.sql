-- models/intermediate/int_payment_summary.sql
-- Payment-type metrics

with payments as (
    select * from {{ ref('stg_payments') }}
),
orders as (
    select order_id, customer_state, customer_city
    from {{ ref('stg_orders') }}
)

select
    p.payment_type,
    o.customer_state,
    o.customer_city,
    
    count(distinct p.order_id) as total_orders,
    sum(p.payment_value) as total_payment_value,
    avg(p.payment_value) as avg_payment_value,
    max(p.payment_installments) as max_installments

from payments p
join orders o on p.order_id = o.order_id
group by 1,2,3
