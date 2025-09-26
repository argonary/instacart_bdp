select *
from {{ ref('inventory_suggestions') }}
where forecast_value < 0
   or prediction_interval_lower < 0
   or prediction_interval_upper < 0
   or target_stock < 0
