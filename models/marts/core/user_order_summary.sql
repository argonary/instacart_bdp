{{ config(materialized='table') }}

with orders as (
  select user_id, order_id from {{ ref('stg_orders') }}
),
foi as (
  select user_id, order_id, product_id, reordered
  from {{ ref('fact_order_item') }}
),
order_sizes as (
  select user_id, order_id, count(*) as basket_size
  from foi
  group by user_id, order_id
),
agg as (
  select
    o.user_id,
    count(distinct o.order_id)                         as orders,
    avg(os.basket_size)                                as avg_basket_size,
    count(distinct product_id)                         as distinct_products,
    sum(case when reordered = 1 then 1 else 0 end)     as items_reordered,
    count(*)                                           as items_total
  from orders o
  join foi f using (user_id, order_id)
  join order_sizes os using (user_id, order_id)
  group by o.user_id
)
select
  u.user_id,
  u.order_count,
  u.avg_days_between_orders,
  u.avg_basket_size                                   as avg_basket_size_dim,
  a.orders,
  a.avg_basket_size                                   as avg_basket_size_calc,
  a.distinct_products,
  a.items_reordered,
  a.items_total,
  safe_divide(a.items_reordered, nullif(a.items_total,0)) as reorder_rate
from {{ ref('dim_user') }} u
left join agg a using (user_id)
