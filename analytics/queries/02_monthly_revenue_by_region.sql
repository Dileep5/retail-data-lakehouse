SELECT to_char(f.order_date, 'YYYY-MM') AS month, s.region, round(sum(f.recognized_revenue), 2) AS revenue
FROM analytics_marts.fct_orders f
JOIN analytics_marts.dim_stores s USING (store_id)
GROUP BY 1, 2
ORDER BY 1, 2;
