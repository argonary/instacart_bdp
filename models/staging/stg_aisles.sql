select aisle_id, aisle
from {{source('instacart', 'aisles')}}