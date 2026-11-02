-- SCD2 in action: tier at time of purchase vs. tier today.
SELECT loyalty_tier_at_order, current_tier, completed_orders, revenue
FROM analytics_reports.rpt_revenue_by_tier_at_order
ORDER BY loyalty_tier_at_order, current_tier;
