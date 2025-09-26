{{ config(
  materialized='table',
  schema='marts',
  partition_by = {"field": "plan_generated_on", "data_type": "date"},
  cluster_by   = ["product_id"]
) }}

{% set horizon_weeks = var('horizon_weeks', 4) %}

-- Rank each SKU's forecast weeks by timestamp
with ranked as (
  select
    product_id,
    product_name,
    date(forecast_timestamp) as week_start,
    row_number() over (partition by product_id order by forecast_timestamp) as rn
  from {{ ref('product_demand_forecast_enriched') }}
),

-- Keep only the first N weeks and bring in targets
h as (
  select
    i.product_id,
    i.product_name,
    i.week_start,
    i.target_stock,
    r.rn
  from {{ ref('inventory_suggestions') }} i
  join ranked r using (product_id, week_start)
  where r.rn <= {{ horizon_weeks }}
),

-- Aggregate to a single row per SKU
agg as (
  select
    product_id,
    any_value(product_name) as product_name,
    sum(target_stock) as order_qty_{{ horizon_weeks }}w,
    sum(case when rn = 1 then target_stock else 0 end) as order_qty_next_week
  from h
  group by product_id
)

select current_date() as plan_generated_on, *
from agg
