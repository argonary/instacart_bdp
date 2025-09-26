{{ config(materialized='view', schema='marts') }}
SELECT *
FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval_rollup(1.30, 10, 1, 0.80, NULL, NULL)
