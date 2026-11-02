-- SCD Type 2 customer dimension built from the dbt snapshot.
-- One row per customer version; customer_sk identifies the version.
select
    dbt_scd_id                                   as customer_sk,
    customer_id,
    first_name || ' ' || last_name               as full_name,
    email,
    city,
    province,
    loyalty_tier,
    signup_date,
    dbt_valid_from                               as valid_from,
    dbt_valid_to                                 as valid_to,
    dbt_valid_to is null                         as is_current,
    row_number() over (partition by customer_id order by dbt_valid_from) as version
from {{ ref('customers_snapshot') }}
