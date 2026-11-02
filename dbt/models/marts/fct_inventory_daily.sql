{{
  config(
    materialized='incremental',
    unique_key=['snapshot_date', 'store_id', 'product_id'],
    incremental_strategy='delete+insert'
  )
}}

select
    snapshot_date,
    store_id,
    product_id,
    quantity_on_hand,
    reorder_point,
    below_reorder_point,
    _batch_id
from {{ ref('stg_inventory') }}
{% if is_incremental() %}
where _batch_id >= (select coalesce(max(_batch_id), '1900-01-01'::date) from {{ this }})
{% endif %}
