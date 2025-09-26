select product_id, week_start, count(*) as c
from {{ ref('inventory_suggestions') }}
group by 1,2
having count(*) > 1
