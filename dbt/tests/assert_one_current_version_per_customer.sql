select customer_id, count(*) filter (where is_current) as current_versions
from {{ ref('dim_customers') }}
group by customer_id
having count(*) filter (where is_current) <> 1
