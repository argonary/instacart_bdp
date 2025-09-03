with op as (select * from {{ref('stg_order_products')}}),
o as (select * from {{ref('stg_orders')}})
SELECT
    o.order_id,
    o.user_id,
    o.eval_set,
    o.order_number,
    o.order_dow,
    o.order_hour_of_day,
    o.days_since_prior_order,
    op.product_id,
    op.add_to_cart_order,
    op.reordered
from op
join o using (order_id)