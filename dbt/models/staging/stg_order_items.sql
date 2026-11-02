select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    {{ line_revenue('quantity', 'unit_price', 'discount_pct') }} as line_revenue,
    _batch_id
from {{ source('lake_silver', 'order_items') }}
