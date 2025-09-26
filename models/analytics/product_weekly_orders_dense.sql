{{ config(materialized='table', schema='marts') }}

-- 1) Weekly calendar from the min to max week we have
with bounds as (
  select
    min(week_start) as min_week,
    max(week_start) as max_week
  from {{ ref('product_weekly_orders') }}
),
calendar as (
  select date_add(b.min_week, interval week_num week) as week_start
  from bounds b,
  unnest(generate_array(0, date_diff(b.max_week, b.min_week, week))) as week_num
),

-- 2) Distinct products (pick any name per id)
products as (
  select
    product_id,
    any_value(product_name) as product_name
  from {{ ref('product_weekly_orders') }}
  group by product_id
),

-- 3) Dense grid: every product x every week
grid as (
  select p.product_id, p.product_name, c.week_start
  from products p
  cross join calendar c
),

-- 4) Left join actual orders and coalesce to zero
joined as (
  select
    g.product_id,
    g.product_name,
    g.week_start,
    coalesce(pwo.orders, 0) as orders
  from grid g
  left join {{ ref('product_weekly_orders') }} pwo
    using (product_id, week_start)
)

select * from joined
