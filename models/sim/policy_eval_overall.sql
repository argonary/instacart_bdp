{{ config(materialized='table', schema='marts') }}

-- Roll up per-week scenario metrics into one row per scenario.
-- Baseline (scenario_id = 0): force all summary KPIs to 0.

with s as (
  select
    scenario_id,
    scenario_label,
    week_start,
    fill_rate_delta_pp,        -- 100 * (policy_fill - baseline_fill)
    cost_reduction_pct         -- SAFE_DIVIDE(100 * (baseline_cost - policy_cost), baseline_cost)
  from {{ ref('policy_eval_summary') }}
),

normalized as (
  select
    scenario_id,
    scenario_label,
    case when scenario_id = 0 then 0.0 else fill_rate_delta_pp end  as fill_rate_delta_pp_norm,
    case when scenario_id = 0 then 0.0 else cost_reduction_pct end  as cost_reduction_pct_norm
  from s
)

select
  scenario_id,
  scenario_label,
  round(avg(fill_rate_delta_pp_norm), 2) as avg_fill_rate_delta_pp,
  round(avg(cost_reduction_pct_norm), 2) as avg_cost_reduction_pct
from normalized
group by 1, 2
