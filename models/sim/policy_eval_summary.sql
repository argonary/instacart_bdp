{{ config(
  materialized = 'table',
  schema       = 'marts',
  partition_by = {"field": "week_start", "data_type": "date"}
) }}

-- Row-level metrics from the simulator
with m as (
  select
    scenario_id,
    scenario_label,
    shortage_penalty,
    holding_cost,
    week_start,
    demand,
    served_baseline,
    served_policy,
    short_baseline,
    short_policy,
    over_baseline,
    over_policy
  from {{ ref('policy_eval_rows') }}
),

-- Aggregate to week x scenario (compute sums first, then fill rates)
agg as (
  select
    scenario_id,
    scenario_label,
    week_start,
    sum(demand)          as sum_demand,
    sum(served_baseline) as sum_served_baseline,
    sum(served_policy)   as sum_served_policy,
    -- costs under this scenario's penalty/holding settings
    sum(shortage_penalty * short_baseline + holding_cost * over_baseline) as cost_baseline,
    sum(shortage_penalty * short_policy   + holding_cost * over_policy)   as cost_policy
  from m
  group by 1,2,3
),

-- Convert sums to fill rates; treat zero/NULL demand as 0 fill instead of NULL
rates as (
  select
    scenario_id,
    scenario_label,
    week_start,
    sum_demand,
    coalesce(safe_divide(sum_served_baseline, sum_demand), 0.0) as fill_rate_baseline,
    coalesce(safe_divide(sum_served_policy,   sum_demand), 0.0) as fill_rate_policy,
    cost_baseline,
    cost_policy
  from agg
),

-- Baseline row (scenario_id = 0) per week for reference
baseline_ref as (
  select
    week_start,
    fill_rate_baseline as base_fill_rate,
    cost_baseline      as base_cost
  from rates
  where scenario_id = 0
)

-- Final per-week, per-scenario summary with deltas vs baseline
select
  r.week_start,
  r.scenario_id,
  r.scenario_label,

  -- scenario metrics
  r.fill_rate_baseline,
  r.fill_rate_policy,
  r.cost_baseline,
  r.cost_policy,

  -- baseline references
  b.base_fill_rate,
  b.base_cost,

  -- headline deltas
  100 * (r.fill_rate_policy - b.base_fill_rate)                                  as fill_rate_delta_pp,        -- robust pp delta
  safe_divide(100 * (r.fill_rate_policy - b.base_fill_rate), nullif(b.base_fill_rate, 0.0))
                                                                                    as fill_rate_improvement_pct, -- relative %
  safe_divide(100 * (b.base_cost - r.cost_policy),           nullif(b.base_cost, 0.0))
                                                                                    as cost_reduction_pct,

  -- include demand so we can do demand-weighted rollups downstream
  r.sum_demand
from rates r
join baseline_ref b using (week_start)
