{{ config(materialized='table') }}

WITH eligible AS (
  SELECT
    o.department_id,
    o.department_name,
    DATE(o.week_start)              AS week_date,
    -- TEMPORARY proxy: use cost_baseline to get the model building
    -- Replace with revenue in version B
    o.cost_baseline                 AS gmv_usd
  FROM {{ ref('exp_opportunities') }} o
),
last_52w AS (
  SELECT *
  FROM eligible
  WHERE week_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 52 WEEK)
)
SELECT
  department_id,
  COALESCE(department_name, 'ALL_DEPARTMENTS') AS department_name,
  SUM(gmv_usd) AS gmv_52w_usd
FROM last_52w
GROUP BY ROLLUP(department_id, department_name)
