-- Uses the SCD2 point-in-time join: revenue is attributed to the tier the customer
-- held when ordering, which can differ from their current tier.
select
    f.loyalty_tier_at_order,
    c_now.loyalty_tier                     as current_tier,
    count(*)                               as completed_orders,
    sum(f.recognized_revenue)              as revenue
from {{ ref('fct_orders') }} f
join {{ ref('dim_customers') }} c_now on c_now.customer_id = f.customer_id and c_now.is_current
where f.is_completed
group by 1, 2
