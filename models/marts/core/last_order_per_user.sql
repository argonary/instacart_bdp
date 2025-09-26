{{ config(materialized='table') }}

with orders as (
  select user_id, order_id, order_number, order_dow, order_hour_of_day, days_since_prior_order, eval_set
  from {{ ref('stg_orders') }}
),
ranked as (
  select
    orders.*,
    row_number() over (partition by user_id order by order_number desc, order_id desc) as rn
  from orders
)
select
  user_id,
  order_id,
  order_number,
  order_dow,
  order_hour_of_day,
  days_since_prior_order,
  eval_set
from ranked
where rn = 1
