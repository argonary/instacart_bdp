{{ config(materialized='table', schema='marts') }}

with agg as (
  select
    product_id,
    count(*) as weeks,
    sum(orders) as total_orders
  from {{ ref('product_weekly_orders_dense') }}
  group by product_id
)

select
  d.*
from {{ ref('product_weekly_orders_dense') }} d
join agg a using (product_id)
where a.weeks >= 40          -- at least ~9–10 months of history
  and a.total_orders >= 200  -- avoid ultra-sparse series
