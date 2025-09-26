{% macro bqml_evaluate_arima(horizon_weeks=4, confidence=0.8) %}
  {% set base_rel = ref('product_weekly_orders_filtered') %}
  {% set project_id = base_rel.database %}
  {% set dataset_id = base_rel.schema %}
  {% set model_name = var('bqml_timeseries_model', 'product_demand_arima') %}

  CREATE OR REPLACE TABLE `{{ project_id }}.{{ dataset_id }}.product_demand_arima_eval` AS
  SELECT *
  FROM ML.EVALUATE(
    MODEL `{{ project_id }}.{{ dataset_id }}.{{ model_name }}`,
    (
      SELECT
        product_id,
        week_start,
        orders
      FROM {{ ref('product_weekly_orders_filtered') }}
    ),
    STRUCT({{ horizon_weeks }} AS horizon, {{ confidence }} AS confidence_level)
  );

  {{ return('created ' ~ project_id ~ '.' ~ dataset_id ~ '.product_demand_arima_eval') }}
{% endmacro %}
