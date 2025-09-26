{{ config(materialized='table') }}

WITH evals AS (
  SELECT * FROM `{{ target.project }}.dbt_instacart_dev_marts`.fn_policy_eval(0.95, 10, 1, 0.0, NULL, NULL)
),
prod AS (
  SELECT product_id, department_id, department_name
  FROM `{{ target.project }}.dbt_instacart_dev_marts.dim_product`
)
SELECT
  p.department_id,
  ANY_VALUE(p.department_name) AS department_name,
  ROUND(100 * AVG(e.fill_rate_policy - e.fill_rate_baseline), 3) AS fill_pp,
  ROUND(100 * SAFE_DIVIDE(SUM(e.cost_baseline) - SUM(e.cost_policy), NULLIF(SUM(e.cost_baseline), 0)), 3) AS cost_pct,
  COUNT(*) AS row_count,
  ROUND(SUM(e.cost_baseline) - SUM(e.cost_policy), 2) AS cost_delta_abs
FROM evals e
LEFT JOIN prod p USING (product_id)
GROUP BY p.department_id
