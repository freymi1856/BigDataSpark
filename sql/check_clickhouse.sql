SELECT 'product_sales_report' AS table_name, count() AS rows_count FROM product_sales_report
UNION ALL
SELECT 'customer_sales_report', count() FROM customer_sales_report
UNION ALL
SELECT 'time_sales_report', count() FROM time_sales_report
UNION ALL
SELECT 'store_sales_report', count() FROM store_sales_report
UNION ALL
SELECT 'supplier_sales_report', count() FROM supplier_sales_report
UNION ALL
SELECT 'product_quality_report', count() FROM product_quality_report;

SELECT
    product_sales_rank,
    product_name,
    product_category,
    sold_quantity,
    total_revenue
FROM product_sales_report
WHERE is_top_10_by_quantity != 0
ORDER BY product_sales_rank;

SELECT
    year,
    month,
    month_name,
    total_revenue,
    previous_month_revenue,
    month_revenue_delta
FROM time_sales_report
ORDER BY year, month;
