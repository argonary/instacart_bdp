{% macro bqml_forecast_arima(horizon_weeks=4, confidence=0.8) %}
  {% set dataset %}{{ target.schema }}_marts{% endset %}
  {% set sql %}
    CREATE OR REPLACE TABLE `{{ target.project }}.{{ dataset }}.product_demand_forecast` AS
    SELECT *
    FROM ML.FORECAST(
      MODEL `{{ target.project }}.{{ dataset }}.product_demand_arima`,
      STRUCT({{ horizon_weeks }} AS horizon, {{ confidence }} AS confidence_level)
    );
  {% endset %}
  {% do run_query(sql) %}
{% endmacro %}
