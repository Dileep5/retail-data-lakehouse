select
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    city,
    province,
    loyalty_tier,
    signup_date,
    updated_at,
    _batch_id
from {{ source('lake_silver', 'customers') }}
