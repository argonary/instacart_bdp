with p as (select * from {{ref('stg_products') }}),
a as (select * from {{ref('stg_aisles')}}),
d as (select * from {{ref('stg_departments')}})
select
    p.product_id,
    p.product_name,
    a.aisle,
    d.department
from p
left join a using (aisle_id)
left join d using (department_id)