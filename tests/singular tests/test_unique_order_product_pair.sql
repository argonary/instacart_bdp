SELECT order_id, product_id, count(*) c
from {{ ref('stg_order_products') }}
group by 1,2
having count(*)>1