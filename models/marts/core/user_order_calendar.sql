with o as (
    SELECT user_id, order_id, order_number, days_since_prior_order
    from {{ref('stg_orders')}}
),
calc as (
    SELECT
        user_id, order_id, order_number, days_since_prior_order,
        sum(coalesce(days_since_prior_order, 0)) over(
            PARTITION by user_id, order_number
            rows between UNBOUNDED PRECEDING AND CURRENT ROW
        ) as days_since_first_order
    FROM o
)
SELECT *,
    floor(days_since_first_order/7) as week_index
from calc 