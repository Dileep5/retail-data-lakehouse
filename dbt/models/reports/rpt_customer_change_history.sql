-- Customers with more than one SCD2 version, showing what changed and when.
select
    customer_id,
    version,
    loyalty_tier,
    city,
    province,
    valid_from,
    valid_to,
    is_current
from {{ ref('dim_customers') }}
where customer_id in (
    select customer_id from {{ ref('dim_customers') }} group by customer_id having count(*) > 1
)
