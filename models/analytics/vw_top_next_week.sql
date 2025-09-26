{{ config(materialized='view', schema='marts') }}
with anchor as (
  select min(forecast_timestamp) as next_week
  from {{ ref('product_demand_forecast_enriched') }}
)
select
  product_id, product_name,
  forecast_timestamp as week_start,
  forecast_value,
  prediction_interval_lower, prediction_interval_upper
from {{ ref('product_demand_forecast_enriched') }}, anchor
where forecast_timestamp = anchor.next_week
order by forecast_value desc
