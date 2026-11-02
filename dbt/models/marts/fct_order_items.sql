{{
  config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='delete+insert'
  )
}}

with items as (
    select * from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    where _batch_id >= (select coalesce(max(_batch_id), '1900-01-01'::date) from {{ this }})
    {% endif %}
)

select
    i.order_item_id,
    i.order_id,
    i.product_id,
    p.category,
    i.quantity,
    i.unit_price,
    i.discount_pct,
    i.line_revenue,
    round((i.quantity * p.unit_cost)::numeric, 2)                  as line_cost,
    i.line_revenue - round((i.quantity * p.unit_cost)::numeric, 2) as line_margin,
    i._batch_id
from items i
left join {{ ref('dim_products') }} p on p.product_id = i.product_id
