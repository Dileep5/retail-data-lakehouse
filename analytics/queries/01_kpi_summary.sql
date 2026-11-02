-- Headline KPIs over the full history.
SELECT
    count(*) FILTER (WHERE is_completed)                                   AS completed_orders,
    round(sum(recognized_revenue), 2)                                      AS revenue,
    round(sum(recognized_revenue) / nullif(count(*) FILTER (WHERE is_completed), 0), 2) AS avg_order_value,
    round(100.0 * count(*) FILTER (WHERE status = 'returned') / count(*), 2) AS return_rate_pct,
    round(100.0 * count(*) FILTER (WHERE channel = 'online' AND is_completed)
          / nullif(count(*) FILTER (WHERE is_completed), 0), 2)             AS online_share_pct
FROM analytics_marts.fct_orders;
