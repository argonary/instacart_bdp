with lines as (
  select
    cast(order_id as int64)          as order_id,
    cast(product_id as int64)        as product_id,
    cast(add_to_cart_order as int64) as add_to_cart_order,
    cast(reordered as int64)         as reordered
  from {{ ref('stg_order_products') }}
),
orders as (
  select
    cast(order_id as int64)                as order_id,
    cast(user_id as int64)                 as user_id,
    cast(order_number as int64)            as order_number,
    cast(order_dow as int64)               as order_dow,
    cast(order_hour_of_day as int64)       as order_hour_of_day,
    cast(days_since_prior_order as int64)  as days_since_prior_order
  from {{ ref('stg_orders') }}
),
dim as (
  select product_id, product_name, aisle_id, aisle_name, department_id, department_name
  from {{ ref('dim_product') }}
)
select
  l.order_id,
  o.user_id,
  l.product_id,
  d.product_name,
  d.aisle_id,
  d.aisle_name,
  d.department_id,
  d.department_name,
  o.order_number,
  o.order_dow,
  o.order_hour_of_day,
  o.days_since_prior_order,
  l.add_to_cart_order,
  l.reordered
from lines l
left join orders o using (order_id)
left join dim d using (product_id)
