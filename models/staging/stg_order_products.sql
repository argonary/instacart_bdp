with prior as (
  select
    cast(order_id as int64)          as order_id,
    cast(product_id as int64)        as product_id,
    cast(add_to_cart_order as int64) as add_to_cart_order,
    cast(reordered as int64)         as reordered,
    'prior'                          as src
  from {{ ref('order_products__prior') }}
),
train as (
  select
    cast(order_id as int64)          as order_id,
    cast(product_id as int64)        as product_id,
    cast(add_to_cart_order as int64) as add_to_cart_order,
    cast(reordered as int64)         as reordered,
    'train'                          as src
  from {{ ref('order_products__train') }}
),
unioned as (
  select * from prior
  union all
  select * from train
),
dedup as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (
        partition by order_id, product_id
        order by add_to_cart_order desc, src
      ) as rn
    from unioned
  )
  where rn = 1
)
select * from dedup
