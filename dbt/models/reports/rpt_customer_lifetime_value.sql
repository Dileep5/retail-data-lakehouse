select
    c.customer_id,
    c.full_name,
    c.loyalty_tier                          as current_tier,
    c.city,
    count(f.order_id) filter (where f.is_completed)            as completed_orders,
    coalesce(sum(f.recognized_revenue), 0)                    as lifetime_revenue,
    round(avg(f.recognized_revenue) filter (where f.is_completed), 2) as avg_order_value,
    min(f.order_date)                       as first_order_date,
    max(f.order_date)                       as last_order_date
from {{ ref('dim_customers') }} c
left join {{ ref('fct_orders') }} f on f.customer_id = c.customer_id
where c.is_current
group by 1, 2, 3, 4
