-- Reconciliation: recognised revenue in the fact table must equal line revenue of
-- completed orders recomputed from staging (tolerance: 1 cent).
with fact as (
    select coalesce(sum(recognized_revenue), 0) as revenue from {{ ref('fct_orders') }}
),
source as (
    select coalesce(sum(i.line_revenue), 0) as revenue
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_orders') }} o using (order_id)
    where o.is_completed
)
select fact.revenue as fact_revenue, source.revenue as staging_revenue
from fact cross join source
where abs(fact.revenue - source.revenue) > 0.01
