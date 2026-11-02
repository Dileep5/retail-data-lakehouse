select
    order_id,
    customer_id,
    store_id,
    order_date,
    channel,
    status,
    status = 'completed' as is_completed,
    created_at,
    updated_at,
    _batch_id
from {{ source('lake_silver', 'orders') }}
