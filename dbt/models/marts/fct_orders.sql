{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
  )
}}
-- Order-grain fact. Incremental with delete+insert, so re-running a batch replaces its
-- rows (idempotent). Each run recomputes:
--   1. orders changed in the latest batch(es);
--   2. older orders that received late-arriving line items (revenue would go stale);
--   3. orders still attributed to the unknown customer, so they are re-attributed
--      once the customer's CRM record arrives (late-arriving dimension).
-- customer_sk is resolved point-in-time against the SCD2 dimension, so revenue can be
-- attributed to the loyalty tier the customer had when ordering.

{% if is_incremental() %}
with watermark as (
    select coalesce(max(_batch_id), '1900-01-01'::date) as batch_id from {{ this }}
),

orders as (
    select o.*
    from {{ ref('stg_orders') }} o
    where o._batch_id >= (select batch_id from watermark)
       or o.order_id in (
            select order_id from {{ ref('stg_order_items') }}
            where _batch_id >= (select batch_id from watermark))
       or o.order_id in (select order_id from {{ this }} where customer_sk = '-1')
),
{% else %}
with orders as (
    select * from {{ ref('stg_orders') }}
),
{% endif %}

items as (
    select
        order_id,
        count(*)          as line_count,
        sum(quantity)     as units,
        sum(line_revenue) as order_revenue
    from {{ ref('stg_order_items') }}
    where order_id in (select order_id from orders)
    group by order_id
),

first_version as (
    select customer_id, customer_sk, loyalty_tier
    from {{ ref('dim_customers') }}
    where version = 1 and customer_id <> -1
)

select
    o.order_id,
    o.order_date,
    o.customer_id,
    coalesce(pit.customer_sk, fv.customer_sk, '-1')                 as customer_sk,
    coalesce(pit.loyalty_tier, fv.loyalty_tier, 'Unknown')          as loyalty_tier_at_order,
    case
        when pit.customer_sk is not null then 'point_in_time'
        when fv.customer_sk is not null then 'first_version'  -- order predates first CRM record
        else 'unknown'                                         -- customer not in CRM yet
    end                                                             as customer_resolution,
    o.store_id,
    o.channel,
    o.status,
    o.is_completed,
    coalesce(i.line_count, 0)                                       as line_count,
    coalesce(i.units, 0)                                            as units,
    coalesce(i.order_revenue, 0)                                    as order_revenue,
    case when o.is_completed then coalesce(i.order_revenue, 0) else 0 end as recognized_revenue,
    o.updated_at,
    o._batch_id
from orders o
left join items i on i.order_id = o.order_id
left join {{ ref('dim_customers') }} pit
    on pit.customer_id = o.customer_id
   and o.order_date >= pit.valid_from::date
   and (pit.valid_to is null or o.order_date < pit.valid_to::date)
left join first_version fv
    on fv.customer_id = o.customer_id
   and pit.customer_sk is null
