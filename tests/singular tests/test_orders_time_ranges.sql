select *
from {{ref('stg_orders')}}
where order_dow not BETWEEN 0 and 6
   or order_hour_of_day not BETWEEN 0 and 24 