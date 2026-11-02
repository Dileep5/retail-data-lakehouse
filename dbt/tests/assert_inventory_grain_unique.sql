select snapshot_date, store_id, product_id, count(*)
from {{ ref('fct_inventory_daily') }}
group by 1, 2, 3
having count(*) > 1
