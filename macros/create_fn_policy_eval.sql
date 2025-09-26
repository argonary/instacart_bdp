-- macros/create_fn_policy_eval.sql

{% macro create_fn_policy_eval() %}
  {{ log("Creating fn_policy_eval...", info=True) }}
  {% set ddl %}
  CREATE OR REPLACE TABLE FUNCTION `instacart-470921.dbt_instacart_dev_marts.fn_policy_eval`(
    service_buffer     FLOAT64,
    shortage_penalty   FLOAT64,
    holding_cost       FLOAT64,
    confidence_level   FLOAT64,     -- 0.80 uses stored PI upper; else mean×buffer
    department_filter  INT64,       -- pass NULL to ignore (placeholder for now)
    aisle_filter       INT64        -- pass NULL to ignore (placeholder for now)
  )
  RETURNS TABLE<
    week_start DATE,
    product_id INT64,
    department_id INT64,
    aisle_id INT64,
    demand INT64,
    order_baseline INT64,
    order_policy INT64,
    served_baseline INT64,
    short_baseline INT64,
    over_baseline INT64,
    served_policy INT64,
    short_policy INT64,
    over_policy INT64,
    fill_rate_baseline FLOAT64,
    fill_rate_policy FLOAT64,
    cost_baseline FLOAT64,
    cost_policy FLOAT64,
    cost_reduction_pct FLOAT64,
    fill_rate_delta_pp FLOAT64,
    confidence_used STRING
  >
  AS (
    {{ instacart__fn_policy_eval_body() }}
  );
  {% endset %}
  {% do run_query(ddl) %}
{% endmacro %}



{% macro instacart__fn_policy_eval_body() %}
  WITH src AS (
    SELECT
      pwof.week_start,
      pwof.product_id,
      COALESCE(CAST(pwof.orders AS INT64), 0) AS demand,
      -- placeholders until you add attrs
      CAST(NULL AS INT64) AS department_id,
      CAST(NULL AS INT64) AS aisle_id,
      f.forecast_value,
      f.prediction_interval_upper
    FROM `instacart-470921.dbt_instacart_dev_marts.product_weekly_orders_filtered` AS pwof
    LEFT JOIN `instacart-470921.dbt_instacart_dev_marts.product_demand_forecast_enriched` AS f
      ON f.product_id = pwof.product_id
     AND DATE(f.forecast_timestamp) = pwof.week_start
  ),
  decisions AS (
    SELECT
      week_start, product_id, department_id, aisle_id, demand,
      CAST(CEIL(COALESCE(forecast_value, 0)) AS INT64) AS order_baseline,
      CAST(
        CEIL(
          CASE
            WHEN ABS(confidence_level - 0.80) < 1e-9
              THEN COALESCE(prediction_interval_upper, forecast_value) * service_buffer
            ELSE COALESCE(forecast_value, 0) * service_buffer
          END
        ) AS INT64
      ) AS order_policy,
      CASE WHEN ABS(confidence_level - 0.80) < 1e-9 THEN 'PI@80' ELSE 'MEAN' END AS confidence_used
    FROM src
  ),
  simulate AS (
    SELECT
      *,
      COALESCE(LEAST(COALESCE(order_baseline,0), COALESCE(demand,0)), 0) AS served_baseline,
      COALESCE(GREATEST(COALESCE(demand,0) - COALESCE(order_baseline,0), 0), 0) AS short_baseline,
      COALESCE(GREATEST(COALESCE(order_baseline,0) - COALESCE(demand,0), 0), 0) AS over_baseline,

      COALESCE(LEAST(COALESCE(order_policy,0), COALESCE(demand,0)), 0) AS served_policy,
      COALESCE(GREATEST(COALESCE(demand,0) - COALESCE(order_policy,0), 0), 0) AS short_policy,
      COALESCE(GREATEST(COALESCE(order_policy,0) - COALESCE(demand,0), 0), 0) AS over_policy
    FROM decisions
  ),
  kpis AS (
    SELECT
      *,
      COALESCE(SAFE_DIVIDE(served_baseline, NULLIF(demand, 0)), 0) AS fill_rate_baseline,
      COALESCE(SAFE_DIVIDE(served_policy,   NULLIF(demand, 0)), 0) AS fill_rate_policy,
      (holding_cost * over_baseline) + (shortage_penalty * short_baseline) AS cost_baseline,
      (holding_cost * over_policy)   + (shortage_penalty * short_policy)   AS cost_policy
    FROM simulate
  )
  SELECT
    week_start,
    product_id,
    department_id,
    aisle_id,
    demand,
    order_baseline,
    order_policy,
    served_baseline,
    short_baseline,
    over_baseline,
    served_policy,
    short_policy,
    over_policy,
    fill_rate_baseline,
    fill_rate_policy,
    cost_baseline,
    cost_policy,
    100 * COALESCE(SAFE_DIVIDE(cost_baseline - cost_policy, NULLIF(cost_baseline, 0)), 0) AS cost_reduction_pct,
    100 * (fill_rate_policy - fill_rate_baseline) AS fill_rate_delta_pp,
    confidence_used
  FROM kpis
{% endmacro %}
