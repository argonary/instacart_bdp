-- models/staging/stg_order_products.sql
select
  cast(order_id as int)            as order_id,
  cast(product_id as int)          as product_id,
  cast(add_to_cart_order as int)   as add_to_cart_order,
  cast(reordered as int)           as reordered
from {{ ref('order_products__prior') }}

union all

select
  cast(order_id as int)            as order_id,
  cast(product_id as int)          as product_id,
  cast(add_to_cart_order as int)   as add_to_cart_order,
  cast(reordered as int)           as reordered
from {{ ref('order_products__train') }}
