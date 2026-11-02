select
    snapshot_date,
    store_id,
    product_id,
    quantity_on_hand,
    reorder_point,
    quantity_on_hand < reorder_point as below_reorder_point,
    _batch_id
from {{ source('lake_silver', 'inventory') }}
