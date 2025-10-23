-- models/intermediate/int_product_performance.sql
with order_items as (
    select *
    from {{ ref('stg_order_items') }}
),
orders as (
    select order_id, customer_city, customer_state
    from {{ ref('stg_orders') }}
),
products as (
    select product_id, product_category_name
    from {{ ref('stg_products') }}
),
reviews as (
    select order_id, product_id, review_score
    from {{ ref('stg_order_reviews') }}
)

select
    p.product_id,
    p.product_category_name,
    o.customer_city,
    o.customer_state,

    -- Metrics
    count(distinct oi.order_id) as total_orders,
    count(*) as total_quantity_sold,
    sum(oi.price + oi.freight_value) as total_revenue,
    avg(oi.price) as avg_price,
    avg(r.review_score) as avg_review_score,

    -- Ranking for top product per city
    row_number() over (partition by o.customer_city order by sum(oi.price + oi.freight_value) desc) as top_product_rank_by_city

from order_items oi
join orders o on oi.order_id = o.order_id
join products p on oi.product_id = p.product_id
left join reviews r on oi.order_id = r.order_id and oi.product_id = r.product_id
group by 1,2,3,4
