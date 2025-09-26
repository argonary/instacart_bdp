{{ config(materialized='table', schema='marts') }}

WITH sweep AS (
  SELECT 'buffer=1.10' AS scenario, * FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval_rollup(
    1.10, {{ var('exp_shortage_penalty') }}, {{ var('exp_holding_cost') }}, {{ var('exp_confidence_level') }}, NULL, NULL)
  UNION ALL
  SELECT 'buffer=1.20' AS scenario, * FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval_rollup(
    1.20, {{ var('exp_shortage_penalty') }}, {{ var('exp_holding_cost') }}, {{ var('exp_confidence_level') }}, NULL, NULL)
  UNION ALL
  SELECT 'buffer=1.30' AS scenario, * FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval_rollup(
    1.30, {{ var('exp_shortage_penalty') }}, {{ var('exp_holding_cost') }}, {{ var('exp_confidence_level') }}, NULL, NULL)
  UNION ALL
  SELECT 'buffer=1.45' AS scenario, * FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval_rollup(
    1.45, {{ var('exp_shortage_penalty') }}, {{ var('exp_holding_cost') }}, {{ var('exp_confidence_level') }}, NULL, NULL)
)
SELECT * FROM sweep
