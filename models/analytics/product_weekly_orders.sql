{{ config(materialized='table', schema='marts') }}

-- Build a synthetic order_date per user by cumulatively summing days_since_prior_order
-- and adding that offset to a fixed anchor date. This yields a consistent global timeline.

with orders_base as (
  select
    o.user_id,
    o.order_id,
    o.order_number,
    cast(o.days_since_prior_order as float64) as days_since_prior_order
  from {{ ref('stg_orders') }} o
),

-- cumulative days before (and including) this order; treat NULL as 0 for first order
orders_dated as (
  select
    user_id,
    order_id,
    order_number,
    -- cumulative offset in days up to this order
    sum(coalesce(days_since_prior_order, 0)) over (
      partition by user_id
      order by order_number
      rows between unbounded preceding and current row
    ) as cum_days
  from orders_base
),

-- pick a fixed anchor date for everyone; add offset
orders_with_date as (
  select
    user_id,
    order_id,
    order_number,
    date_add(date '2016-01-01', interval cast(cum_days as int64) day) as order_date
  from orders_dated
),

-- join to lines and product dim
lines as (
  select
    f.user_id,
    f.order_id,
    f.product_id,
    1 as qty
  from {{ ref('fact_order_item') }} f
),

joined as (
  select
    l.product_id,
    d.product_name,
    date_trunc(o.order_date, week(monday)) as week_start,
    l.qty
  from lines l
  join orders_with_date o using (user_id, order_id)
  join {{ ref('dim_product') }} d using (product_id)
)

select
  product_id,
  product_name,
  week_start,
  sum(qty) as orders
from joined
group by 1,2,3
