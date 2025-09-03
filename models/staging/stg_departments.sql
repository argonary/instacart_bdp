select department_id, department
from {{source('instacart', 'departments')}}