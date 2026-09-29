-- Every order must have a strictly positive quantity
select order_id, quantity
from {{ ref('stg_orders') }}
where quantity is null or quantity <= 0
