SELECT category, sum(units_sold) AS units, round(sum(revenue), 2) AS revenue,
       round(sum(gross_margin), 2) AS gross_margin,
       round(100 * sum(gross_margin) / nullif(sum(revenue), 0), 1) AS margin_pct
FROM analytics_reports.rpt_monthly_category_performance
GROUP BY category
ORDER BY revenue DESC;
