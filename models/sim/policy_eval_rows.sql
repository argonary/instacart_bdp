{{ config(
  materialized = 'table',
  schema       = 'marts',
  partition_by = {"field": "week_start", "data_type": "date"}
) }}

-- Scenario knobs
with scenarios as (
  select
    scenario_id,
    scenario_label,
    service_buffer,
    shortage_penalty,
    holding_cost
  from {{ ref('scenarios') }}
),

-- Weekly demand
demand as (
  select
    product_id,
    week_start,            -- DATE
    orders as demand
  from {{ ref('product_weekly_orders_filtered') }}
),

-- Weekly forecasts (mean + upper PI)
fc as (
  select
    product_id,
    DATE(forecast_timestamp) as week_start,
    forecast_value,
    prediction_interval_upper as pi_upper   -- ✅ correct column name
  from {{ ref('product_demand_forecast_enriched') }}
),

-- Join demand to forecast
grid as (
  select
    d.product_id,
    d.week_start,
    d.demand,
    fc.forecast_value,
    fc.pi_upper
  from demand d
  left join fc using (product_id, week_start)
),

-- Compute baseline & policy orders for each scenario
orders as (
  select
    s.scenario_id,
    s.scenario_label,
    s.service_buffer,
    s.shortage_penalty,
    s.holding_cost,

    g.product_id,
    g.week_start,
    g.demand,

    -- Baseline: mean forecast (ceil)
    CEIL(COALESCE(g.forecast_value, 0)) as baseline_order,

    -- Policy: upper PI (fallback to mean) * buffer
    CEIL(COALESCE(g.pi_upper, g.forecast_value, 0) * s.service_buffer) as policy_order
  from grid g
  cross join scenarios s
)

-- Per-product-week outcomes
select
  scenario_id,
  scenario_label,
  shortage_penalty,
  holding_cost,
  week_start,
  product_id,
  demand,

  -- baseline outcomes
  LEAST(demand, baseline_order)        as served_baseline,
  GREATEST(demand - baseline_order, 0) as short_baseline,
  GREATEST(baseline_order - demand, 0) as over_baseline,

  -- policy outcomes
  LEAST(demand, policy_order)          as served_policy,
  GREATEST(demand - policy_order, 0)   as short_policy,
  GREATEST(policy_order - demand, 0)   as over_policy
from orders
