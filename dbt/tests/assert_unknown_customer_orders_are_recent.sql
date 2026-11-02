-- Orders may wait for their customer's CRM record (late-arriving dimension), but only
-- briefly: anything unresolved for more than 2 days points to a real data problem.
select f.order_id, f.order_date, f.customer_id
from {{ ref('fct_orders') }} f
where f.customer_resolution = 'unknown'
  and f.order_date < (select max(order_date) from {{ ref('fct_orders') }}) - 2
