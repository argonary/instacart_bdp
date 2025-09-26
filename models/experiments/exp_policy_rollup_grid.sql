{{ config(materialized='table', schema='marts') }}

WITH b110 AS (
  SELECT 'buffer=1.10' AS scenario, 1.10 AS service_buffer, r.*
  FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval_rollup(1.10, 10, 1, 0.80, NULL, NULL) AS r
),
b120 AS (
  SELECT 'buffer=1.20' AS scenario, 1.20 AS service_buffer, r.*
  FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval_rollup(1.20, 10, 1, 0.80, NULL, NULL) AS r
),
b130 AS (
  SELECT 'buffer=1.30' AS scenario, 1.30 AS service_buffer, r.*
  FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval_rollup(1.30, 10, 1, 0.80, NULL, NULL) AS r
),
b140 AS (
  SELECT 'buffer=1.40' AS scenario, 1.40 AS service_buffer, r.*
  FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval_rollup(1.40, 10, 1, 0.80, NULL, NULL) AS r
),
b145 AS (
  SELECT 'buffer=1.45' AS scenario, 1.45 AS service_buffer, r.*
  FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval_rollup(1.45, 10, 1, 0.80, NULL, NULL) AS r
)
SELECT * FROM b110
UNION ALL SELECT * FROM b120
UNION ALL SELECT * FROM b130
UNION ALL SELECT * FROM b140
UNION ALL SELECT * FROM b145
