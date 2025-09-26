{% macro bqml_train_arima() %}
  {% set dataset %}{{ target.schema }}_marts{% endset %}  {# e.g., dbt_instacart_dev_marts #}
  {% set sql %}
    CREATE OR REPLACE MODEL `{{ target.project }}.{{ dataset }}.product_demand_arima`
    OPTIONS (
      model_type = 'ARIMA_PLUS',
      time_series_timestamp_col = 'week_start',
      time_series_data_col = 'orders',
      time_series_id_col = 'product_id',
      horizon = 4,
      auto_arima = TRUE
    ) AS
    SELECT
      product_id,
      week_start,
      orders
    FROM `{{ target.project }}.{{ dataset }}.product_weekly_orders`
    WHERE week_start < DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
  {% endset %}
  {% do run_query(sql) %}
{% endmacro %}
