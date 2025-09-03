with ranked as (
  select
    user_id,
    order_id,
    order_number,
    row_number() over (
      partition by user_id
      order by order_number desc, order_id desc
    ) as rn
  from {{ ref('stg_orders') }}
  where order_number is not null
)
select user_id, order_id as holdout_order_id
from ranked
where rn = 1;
