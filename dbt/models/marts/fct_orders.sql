{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
  )
}}
-- Order-grain fact. Incremental: only orders touched by the latest batch(es) are
-- recomputed; re-running a batch replaces its rows (delete+insert) -> idempotent.
-- customer_sk is resolved point-in-time against the SCD2 dimension, so revenue can be
-- attributed to the loyalty tier the customer had when the order was placed.

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where _batch_id >= (select coalesce(max(_batch_id), '1900-01-01'::date) from {{ this }})
    {% endif %}
),

items as (
    select
        order_id,
        count(*)          as line_count,
        sum(quantity)     as units,
        sum(line_revenue) as order_revenue
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.order_date,
    o.customer_id,
    c.customer_sk,
    c.loyalty_tier                                         as loyalty_tier_at_order,
    o.store_id,
    o.channel,
    o.status,
    o.is_completed,
    coalesce(i.line_count, 0)                              as line_count,
    coalesce(i.units, 0)                                   as units,
    coalesce(i.order_revenue, 0)                           as order_revenue,
    case when o.is_completed then coalesce(i.order_revenue, 0) else 0 end as recognized_revenue,
    o.updated_at,
    o._batch_id
from orders o
left join items i on i.order_id = o.order_id
left join {{ ref('dim_customers') }} c
    on c.customer_id = o.customer_id
   and o.order_date >= c.valid_from::date
   and (c.valid_to is null or o.order_date < c.valid_to::date)
