{{ config(materialized='table') }}

WITH cv_thresholds AS (
  SELECT * FROM UNNEST([30, 40, 50, 60]) AS cv_pct
),
cv_lookup AS (
  SELECT
    cv_thresholds.cv_pct,
    APPROX_QUANTILES(v.cv_demand, 101)[OFFSET(cv_thresholds.cv_pct)] AS thr_cv
  FROM `{{ target.project }}.dbt_instacart_dev_marts.exp_volatile_skus` AS v, cv_thresholds
  GROUP BY cv_thresholds.cv_pct
),
lowrisk AS (
  SELECT l.cv_pct, v.product_id
  FROM `{{ target.project }}.dbt_instacart_dev_marts.exp_volatile_skus` AS v
  JOIN cv_lookup AS l
    ON v.cv_demand <= l.thr_cv
),
evals AS (
  SELECT
    l.cv_pct,
    e.*
  FROM lowrisk AS l
  JOIN `{{ target.project }}.dbt_instacart_dev_marts`.fn_policy_eval(
      0.95,   -- service_buffer
      10,     -- shortage_penalty
      1,      -- holding_cost
      0.0,    -- mean
      NULL,
      NULL
  ) AS e
  USING (product_id)
)
SELECT
  e.cv_pct,
  ROUND(100 * AVG(e.fill_rate_policy - e.fill_rate_baseline), 3) AS fill_pp,
  ROUND(100 * SAFE_DIVIDE(SUM(e.cost_baseline) - SUM(e.cost_policy),
                          NULLIF(SUM(e.cost_baseline), 0)), 3)   AS cost_pct,
  COUNT(*)                                                        AS row_count,
  COUNTIF(e.order_policy != e.order_baseline)                     AS rows_changed,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(e.order_policy != e.order_baseline), COUNT(*)), 2) AS pct_changed,
  ROUND(SUM(e.cost_baseline) - SUM(e.cost_policy), 2)            AS cost_delta_abs
FROM evals AS e
GROUP BY 1
