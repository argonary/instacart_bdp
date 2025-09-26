with src as (
  select
    cast(order_id as int64)                as order_id,
    cast(user_id as int64)                 as user_id,
    cast(order_number as int64)            as order_number,
    cast(order_dow as int64)               as order_dow,
    cast(order_hour_of_day as int64)       as order_hour_of_day,
    cast(days_since_prior_order as int64)  as days_since_prior_order,
    cast(eval_set as string)               as eval_set
  from {{ source('instacart','orders') }}
),
dedup as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (
        partition by order_id
        order by order_number desc
      ) as rn
    from src
  )
  where rn = 1
)
select * from dedup
