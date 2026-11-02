select order_id, order_revenue from {{ ref('fct_orders') }} where order_revenue < 0
