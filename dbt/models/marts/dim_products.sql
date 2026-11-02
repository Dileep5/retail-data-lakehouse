-- SCD Type 1: product attributes are overwritten with the latest values.
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
from {{ ref('stg_products') }}
