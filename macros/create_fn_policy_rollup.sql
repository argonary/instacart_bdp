{% macro create_fn_policy_eval_rollup() %}
  {{ log("Creating fn_policy_eval_rollup...", info=True) }}
  {% set ddl %}
  CREATE OR REPLACE TABLE FUNCTION `instacart-470921.dbt_instacart_dev_marts.fn_policy_eval_rollup`(
    service_buffer FLOAT64,
    shortage_penalty FLOAT64,
    holding_cost FLOAT64,
    confidence_level FLOAT64,
    department_filter INT64,
    aisle_filter INT64
  )
  RETURNS TABLE<week_start DATE, row_count INT64, fill_rate_delta_pp FLOAT64, cost_reduction_pct FLOAT64>
  AS (
    SELECT
      week_start,
      COUNT(*) AS row_count,
      ROUND(AVG(100 * (fill_rate_policy - fill_rate_baseline)), 2) AS fill_rate_delta_pp,
      ROUND(AVG(100 * COALESCE(SAFE_DIVIDE(cost_baseline - cost_policy, NULLIF(cost_baseline, 0)), 0)), 2) AS cost_reduction_pct
    FROM `instacart-470921.dbt_instacart_dev_marts`.fn_policy_eval(
      service_buffer, shortage_penalty, holding_cost,
      confidence_level, department_filter, aisle_filter
    )
    GROUP BY week_start
  );
  {% endset %}
  {% do run_query(ddl) %}
{% endmacro %}
