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
  department_id,  -- will be NULL until attrs are added later
  ROUND(AVG(100 * (fill_rate_policy - fill_rate_baseline)), 2) AS avg_fill_delta_pp,
  ROUND(AVG(100 * COALESCE(SAFE_DIVIDE(cost_baseline - cost_policy, NULLIF(cost_baseline,0)),0)), 2) AS avg_cost_pct
FROM eval
GROUP BY department_id
