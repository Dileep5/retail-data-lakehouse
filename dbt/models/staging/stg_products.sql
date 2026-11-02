select
    product_id,
    sku,
    product_name,
    category,
    brand,
    unit_price,
    unit_cost,
    is_active,
    updated_at
from {{ source('lake_silver', 'products') }}
