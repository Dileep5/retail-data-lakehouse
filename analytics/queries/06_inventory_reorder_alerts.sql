SELECT store_name, sku, product_name, quantity_on_hand, reorder_point, units_short
FROM analytics_reports.rpt_inventory_reorder_alerts
ORDER BY units_short DESC, store_name
LIMIT 15;
