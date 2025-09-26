{{ config(materialized='table') }}

WITH eligible AS (
  -- Replace with your “opportunity” logic:
  -- e.g., low CV, baseline overstock, integer-edge, etc.
  SELECT
    o.department_id,
    o.department_name,
    o.week,
    -- Use your best available revenue proxy:
    -- prefer baseline_revenue; else baseline_units * unit_price
    o.baseline_revenue_usd AS gmv_usd
  FROM {{ ref('exp_opportunities') }} o
  -- If exp_opportunities doesn't have revenue, join to your weekly orders/prices mart here.
  -- WHERE o.cv_pct <= 0.4 AND o.was_overstocked = TRUE  -- example filters
),
last_52w AS (
  SELECT *
  FROM eligible
  WHERE week >= DATE_SUB(CURRENT_DATE(), INTERVAL 52 WEEK)
)
SELECT
  department_id,
  department_name,
  SUM(gmv_usd) AS gmv_52w_usd
FROM last_52w
GROUP BY 1,2
UNION ALL
SELECT NULL, 'ALL_DEPARTMENTS', SUM(gmv_usd)
FROM last_52w;
