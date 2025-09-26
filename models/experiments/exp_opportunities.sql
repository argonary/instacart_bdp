{{ config(
    materialized='table',
    partition_by={'field': 'week_start', 'data_type': 'date'}
) }}

WITH evals AS (
  SELECT * FROM `{{ target.project }}.dbt_instacart_dev_marts`.fn_policy_eval(0.95, 10, 1, 0.0, NULL, NULL)
),
prod AS (
  SELECT product_id, product_name, department_id, department_name, aisle_id, aisle_name
  FROM `{{ target.project }}.dbt_instacart_dev_marts.dim_product`
)
SELECT
  e.week_start,
  e.product_id,
  p.product_name,
  p.department_id,
  p.department_name,
  p.aisle_id,
  p.aisle_name,
  e.demand,
  e.served_baseline, e.short_baseline, e.over_baseline,
  e.served_policy,   e.short_policy,   e.over_policy,
  e.order_baseline, e.order_policy,
  (e.order_policy - e.order_baseline) AS delta_order_qty,
  e.cost_baseline, e.cost_policy,
  (e.cost_baseline - e.cost_policy)   AS cost_delta,
  (e.order_policy != e.order_baseline) AS changed_row,
  (e.over_baseline > 0)                AS was_overstocked
FROM evals e
LEFT JOIN prod p USING (product_id)
WHERE (e.order_policy != e.order_baseline) OR (e.over_baseline > 0)
