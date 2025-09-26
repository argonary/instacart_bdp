{{ config(materialized='table') }}

with foi as (
  select product_id, reordered
  from {{ ref('fact_order_item') }}
),
agg as (
  select
    product_id,
    count(*)                                   as item_events,         -- times a product appeared in baskets
    sum(case when reordered = 1 then 1 else 0 end) as item_reorders
  from foi
  group by product_id
)
select
  d.product_id,
  d.product_name,
  d.aisle_id,
  d.aisle_name,
  d.department_id,
  d.department_name,
  a.item_events,
  a.item_reorders,
  safe_divide(a.item_reorders, nullif(a.item_events,0)) as reorder_rate
from agg a
join {{ ref('dim_product') }} d using (product_id)
