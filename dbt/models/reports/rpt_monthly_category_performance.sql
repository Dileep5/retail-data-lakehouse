select
    to_char(o.order_date, 'YYYY-MM')       as year_month,
    i.category,
    sum(i.quantity)                        as units_sold,
    sum(i.line_revenue)                    as revenue,
    sum(i.line_margin)                     as gross_margin,
    round(sum(i.line_margin) / nullif(sum(i.line_revenue), 0), 4) as gross_margin_pct
from {{ ref('fct_order_items') }} i
join {{ ref('fct_orders') }} o using (order_id)
where o.is_completed
group by 1, 2
