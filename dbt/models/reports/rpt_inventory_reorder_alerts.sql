with latest as (
    select max(snapshot_date) as snapshot_date from {{ ref('fct_inventory_daily') }}
)
select
    inv.snapshot_date,
    s.store_name,
    p.sku,
    p.product_name,
    p.category,
    inv.quantity_on_hand,
    inv.reorder_point,
    inv.reorder_point - inv.quantity_on_hand as units_short
from {{ ref('fct_inventory_daily') }} inv
join latest using (snapshot_date)
join {{ ref('dim_stores') }} s using (store_id)
join {{ ref('dim_products') }} p using (product_id)
where inv.below_reorder_point
