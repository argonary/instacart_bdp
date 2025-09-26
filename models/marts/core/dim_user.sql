{{ config(materialized='table') }}

with orders as (
  select
    user_id,
    order_id,
    order_number,
    cast(days_since_prior_order as float64) as days_since_prior_order
  from {{ ref('stg_orders') }}
),

order_aggs as (
  select
    user_id,
    count(*)                                                          as order_count,
    min(order_number)                                                 as first_order_number,
    max(order_number)                                                 as last_order_number,
    ARRAY_AGG(order_id ORDER BY order_number ASC  LIMIT 1)[SAFE_OFFSET(0)] as first_order_id,
    ARRAY_AGG(order_id ORDER BY order_number DESC LIMIT 1)[SAFE_OFFSET(0)] as last_order_id,
    sum(ifnull(days_since_prior_order,0))                              as total_days_since_prior_order,
    avg(days_since_prior_order)                                        as avg_days_between_orders
  from orders
  group by user_id
),

foi as (
  select
    user_id,
    order_id,
    count(*)                               as basket_size,
    countif(reordered = 1)                 as items_reordered_in_order
  from {{ ref('fact_order_item') }}
  group by user_id, order_id
),

basket_aggs as (
  select
    user_id,
    avg(basket_size)                              as avg_basket_size,
    approx_quantiles(basket_size, 100)[offset(50)] as p50_basket_size,
    max(basket_size)                              as max_basket_size
  from foi
  group by user_id
),

product_aggs as (
  select
    user_id,
    count(distinct product_id)               as distinct_products_lifetime,
    sum(case when reordered = 1 then 1 else 0 end) as items_reordered_lifetime,
    count(*)                                 as items_total_lifetime
  from {{ ref('fact_order_item') }}
  group by user_id
)

select
  o.user_id,

  -- ordering trajectory
  o.order_count,
  o.first_order_number,
  o.last_order_number,
  o.first_order_id,
  o.last_order_id,

  -- cadence
  o.total_days_since_prior_order,
  o.avg_days_between_orders,

  -- basket behavior
  b.avg_basket_size,
  b.p50_basket_size,
  b.max_basket_size,

  -- variety & loyalty
  p.distinct_products_lifetime,
  p.items_total_lifetime,
  p.items_reordered_lifetime,
  safe_divide(p.items_reordered_lifetime, nullif(p.items_total_lifetime,0)) as reorder_rate_lifetime
from order_aggs o
left join basket_aggs  b using (user_id)
left join product_aggs p using (user_id)
