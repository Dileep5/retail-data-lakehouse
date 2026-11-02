SELECT customer_id, full_name, current_tier, completed_orders, lifetime_revenue, avg_order_value
FROM analytics_reports.rpt_customer_lifetime_value
ORDER BY lifetime_revenue DESC
LIMIT 10;
