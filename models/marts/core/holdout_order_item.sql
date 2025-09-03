with f as (select * from {{ ref('fact_order_item') }}),
     h as (select * from {{ ref('last_order_per_user') }})
select f.*
from f
join h
  on f.user_id = h.user_id
 and f.order_id = h.holdout_order_id;
