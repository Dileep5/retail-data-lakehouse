-- Fails if two versions of the same customer overlap in time.
select a.customer_id, a.customer_sk, b.customer_sk as overlapping_sk
from {{ ref('dim_customers') }} a
join {{ ref('dim_customers') }} b
  on a.customer_id = b.customer_id
 and a.customer_sk <> b.customer_sk
 and a.valid_from < coalesce(b.valid_to, 'infinity'::timestamp)
 and b.valid_from < coalesce(a.valid_to, 'infinity'::timestamp)
