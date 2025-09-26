{{ config(materialized='table') }}

with p as (
  select product_id, product_name, aisle_id, department_id
  from {{ ref('stg_products') }}
),
a as (
  select aisle_id, aisle as aisle_name
  from {{ ref('stg_aisles') }}
),
d as (
  select department_id, department as department_name
  from {{ ref('stg_departments') }}
)
select
  p.product_id,
  p.product_name,
  a.aisle_id,
  a.aisle_name,
  d.department_id,
  d.department_name
from p
left join a using (aisle_id)
left join d using (department_id)
