select f.order_id, f.order_date, s.store_name, s.opened_date
from {{ ref('fct_orders') }} f
join {{ ref('dim_stores') }} s using (store_id)
where f.order_date < s.opened_date
