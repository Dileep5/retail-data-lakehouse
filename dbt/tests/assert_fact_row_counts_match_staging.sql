select 'orders' as entity, (select count(*) from {{ ref('fct_orders') }}) as fact_rows,
       (select count(*) from {{ ref('stg_orders') }}) as staging_rows
where (select count(*) from {{ ref('fct_orders') }}) <> (select count(*) from {{ ref('stg_orders') }})
union all
select 'order_items', (select count(*) from {{ ref('fct_order_items') }}),
       (select count(*) from {{ ref('stg_order_items') }})
where (select count(*) from {{ ref('fct_order_items') }}) <> (select count(*) from {{ ref('stg_order_items') }})
