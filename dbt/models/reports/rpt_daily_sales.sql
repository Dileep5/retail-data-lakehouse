select
    f.order_date,
    s.store_name,
    s.region,
    count(*)                                         as orders,
    count(*) filter (where f.status = 'completed')   as completed_orders,
    count(*) filter (where f.status = 'returned')    as returned_orders,
    sum(f.units) filter (where f.is_completed)       as units_sold,
    sum(f.recognized_revenue)                        as revenue
from {{ ref('fct_orders') }} f
join {{ ref('dim_stores') }} s using (store_id)
group by 1, 2, 3
