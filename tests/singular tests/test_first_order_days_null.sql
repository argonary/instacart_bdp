SELECT *
from {{ref('stg_orders')}}
where (order_number = 1 and days_since_prior_order is not null)
   or (order_number > 1 and days_since_prior_order is NULL)