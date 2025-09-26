{{ config(materialized='table', schema='marts') }}

WITH eval AS (
  SELECT *
  FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval(
    {{ var('exp_service_buffer') }},
    {{ var('exp_shortage_penalty') }},
    {{ var('exp_holding_cost') }},
    {{ var('exp_confidence_level') }},
    NULL, NULL
  )
)
SELECT
  week_start,
  SAFE_DIVIDE(SUM((fill_rate_policy - fill_rate_baseline) * demand), SUM(demand)) * 100 AS demand_weighted_fill_pp
FROM eval
GROUP BY week_start
