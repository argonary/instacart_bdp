{{ config(materialized='table') }}

-- Weekly cost/fill deltas for a small set of scenarios (mean forecast only).
-- BigQuery table functions cannot be correlated to columns, so we call fn_policy_eval
-- with literal constants and UNION ALL the scenarios.

-- Edit the list below if you want more/other buffers.
WITH scenarios AS (
  SELECT 'mean x0.80' AS scenario_label, 0.80 AS service_buffer UNION ALL
  SELECT 'mean x0.95', 0.95 UNION ALL
  SELECT 'mean x1.00', 1.00 UNION ALL
  SELECT 'mean x1.10', 1.10 UNION ALL
  SELECT 'mean x1.30', 1.30 UNION ALL
  SELECT 'mean x1.60', 1.60
),

-- One SELECT per scenario label to avoid correlating TVFs.
wk_080 AS (
  SELECT 'mean x0.80' AS scenario_label, 0.80 AS service_buffer, e.week_start,
         100 * (AVG(e.fill_rate_policy) - AVG(e.fill_rate_baseline)) AS fill_pp,
         100 * SAFE_DIVIDE(SUM(e.cost_baseline) - SUM(e.cost_policy),
                           NULLIF(SUM(e.cost_baseline), 0)) AS cost_pct
  FROM `{{ target.project }}.dbt_instacart_dev_marts`.fn_policy_eval(0.80, 10, 1, 0.0, NULL, NULL) AS e
  GROUP BY week_start
),
wk_095 AS (
  SELECT 'mean x0.95', 0.95, e.week_start,
         100 * (AVG(e.fill_rate_policy) - AVG(e.fill_rate_baseline)) AS fill_pp,
         100 * SAFE_DIVIDE(SUM(e.cost_baseline) - SUM(e.cost_policy),
                           NULLIF(SUM(e.cost_baseline), 0)) AS cost_pct
  FROM `{{ target.project }}.dbt_instacart_dev_marts`.fn_policy_eval(0.95, 10, 1, 0.0, NULL, NULL) AS e
  GROUP BY week_start
),
wk_100 AS (
  SELECT 'mean x1.00', 1.00, e.week_start,
         100 * (AVG(e.fill_rate_policy) - AVG(e.fill_rate_baseline)) AS fill_pp,
         100 * SAFE_DIVIDE(SUM(e.cost_baseline) - SUM(e.cost_policy),
                           NULLIF(SUM(e.cost_baseline), 0)) AS cost_pct
  FROM `{{ target.project }}.dbt_instacart_dev_marts`.fn_policy_eval(1.00, 10, 1, 0.0, NULL, NULL) AS e
  GROUP BY week_start
),
wk_110 AS (
  SELECT 'mean x1.10', 1.10, e.week_start,
         100 * (AVG(e.fill_rate_policy) - AVG(e.fill_rate_baseline)) AS fill_pp,
         100 * SAFE_DIVIDE(SUM(e.cost_baseline) - SUM(e.cost_policy),
                           NULLIF(SUM(e.cost_baseline), 0)) AS cost_pct
  FROM `{{ target.project }}.dbt_instacart_dev_marts`.fn_policy_eval(1.10, 10, 1, 0.0, NULL, NULL) AS e
  GROUP BY week_start
),
wk_130 AS (
  SELECT 'mean x1.30', 1.30, e.week_start,
         100 * (AVG(e.fill_rate_policy) - AVG(e.fill_rate_baseline)) AS fill_pp,
         100 * SAFE_DIVIDE(SUM(e.cost_baseline) - SUM(e.cost_policy),
                           NULLIF(SUM(e.cost_baseline), 0)) AS cost_pct
  FROM `{{ target.project }}.dbt_instacart_dev_marts`.fn_policy_eval(1.30, 10, 1, 0.0, NULL, NULL) AS e
  GROUP BY week_start
),
wk_160 AS (
  SELECT 'mean x1.60', 1.60, e.week_start,
         100 * (AVG(e.fill_rate_policy) - AVG(e.fill_rate_baseline)) AS fill_pp,
         100 * SAFE_DIVIDE(SUM(e.cost_baseline) - SUM(e.cost_policy),
                           NULLIF(SUM(e.cost_baseline), 0)) AS cost_pct
  FROM `{{ target.project }}.dbt_instacart_dev_marts`.fn_policy_eval(1.60, 10, 1, 0.0, NULL, NULL) AS e
  GROUP BY week_start
),

unioned AS (
  SELECT * FROM wk_080 UNION ALL
  SELECT * FROM wk_095 UNION ALL
  SELECT * FROM wk_100 UNION ALL
  SELECT * FROM wk_110 UNION ALL
  SELECT * FROM wk_130 UNION ALL
  SELECT * FROM wk_160
)

SELECT
  s.scenario_label,
  u.service_buffer,
  u.week_start,
  ROUND(u.fill_pp, 4)  AS fill_pp,
  ROUND(u.cost_pct, 4) AS cost_pct,
  ROUND(AVG(u.cost_pct) OVER (
        PARTITION BY s.scenario_label
        ORDER BY u.week_start
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
  ), 4) AS cost_pct_8w_avg
FROM unioned u
JOIN scenarios s
  ON s.service_buffer = u.service_buffer
