-- SCD Type 2 customer dimension built from the dbt snapshot.
-- One row per customer version; customer_sk identifies the version.
-- customer_sk '-1' is the "unknown member" used for facts whose customer has not
-- arrived from the CRM yet (late-arriving dimension); see fct_orders.
with versions as (
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
)

select * from versions
union all
select
    '-1', -1, 'Unknown customer', null, null, null, 'Unknown', null,
    '1900-01-01'::timestamp, null::timestamp, true, 1
