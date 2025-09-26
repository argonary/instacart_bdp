
select *
from {{ref ('stg_order_products')}}
where reordered not in (0,1)