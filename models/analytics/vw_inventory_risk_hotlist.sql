{{ config(materialized='view', schema='marts') }}
select
  product_id, product_name, week_start,
  forecast_value, baseline_8w, target_stock,
  risk_ratio, risk_flag
from {{ ref('inventory_suggestions') }}
where risk_flag = true
order by risk_ratio desc
