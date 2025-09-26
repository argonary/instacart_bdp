{% set base_rel   = ref('product_weekly_orders_filtered') %}
{{ config(materialized='table', schema='marts') }}

{% set service_buffer = var('service_buffer', 1.20) %}
{% set risk_multiplier = var('risk_multiplier', 1.30) %}
{% set horizon_weeks   = var('horizon_weeks', 4) %}

with anchors as (
  -- Per-SKU first forecast week (DATE) for baseline window
  select product_id, date(min(forecast_timestamp)) as next_week
  from {{ ref('product_demand_forecast_enriched') }}
  group by product_id
),
baseline as (
  -- 8-week trailing avg BEFORE each SKU's next_week
  select
    w.product_id,
    avg(w.orders) as avg_orders_8w
  from {{ ref('product_weekly_orders_filtered') }} w
  join anchors a using (product_id)
  where w.week_start >= date_sub(a.next_week, interval 8 week)
    and w.week_start <  a.next_week
  group by 1
),
ranked as (
  -- Rank forecasts per SKU and keep the first N rows by timestamp
  select
    e.product_id,
    e.product_name,
    date(e.forecast_timestamp) as week_start,
    e.forecast_value,
    e.prediction_interval_lower,
    e.prediction_interval_upper,
    row_number() over (partition by e.product_id order by e.forecast_timestamp) as rn
  from {{ ref('product_demand_forecast_enriched') }} e
),
horizon as (
  select
    product_id,
    product_name,
    week_start,
    forecast_value,
    prediction_interval_lower,
    prediction_interval_upper
  from ranked
  where rn <= {{ horizon_weeks }}
),
scored as (
  select
    h.*,
    coalesce(b.avg_orders_8w, 0)                               as baseline_8w,
    ceil(h.forecast_value * {{ service_buffer }})              as target_stock,
    safe_divide(h.prediction_interval_upper,
                nullif(coalesce(b.avg_orders_8w, 0), 0))       as risk_ratio,
    case
      when coalesce(b.avg_orders_8w, 0) = 0 then false
      when h.prediction_interval_upper >
           coalesce(b.avg_orders_8w, 0) * {{ risk_multiplier }}
      then true else false
    end                                                        as risk_flag,
    row_number() over (partition by h.week_start order by h.forecast_value desc) as rank_this_week
  from horizon h
  left join baseline b using (product_id)
)
select * from scored
