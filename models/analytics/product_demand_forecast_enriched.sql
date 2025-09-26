{% set base_rel   = ref('product_weekly_orders_filtered') %}
{% set project_id = base_rel.database %}
{% set dataset_id = base_rel.schema %}

-- Build into the marts dataset (dbt will expand to dbt_instacart_dev_marts)
{{ config(
  materialized='table',
  schema='marts',
  partition_by = {"field": "forecast_timestamp", "data_type": "timestamp"},
  cluster_by   = ["product_id"]
) }}


with f as (
  select
    product_id,
    forecast_timestamp,
    forecast_value,
    prediction_interval_lower_bound  as prediction_interval_lower,
    prediction_interval_upper_bound  as prediction_interval_upper
  from `{{ project_id }}.{{ dataset_id }}.product_demand_forecast`
)
select
  f.product_id,
  p.product_name,
  -- (add aisle/department later once we confirm exact column names)
  f.forecast_timestamp,
  f.forecast_value,
  f.prediction_interval_lower,
  f.prediction_interval_upper
from f
left join {{ ref('dim_product') }} as p
  on p.product_id = f.product_id
