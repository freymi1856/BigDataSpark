SELECT COUNT(*) AS raw_rows
FROM raw.mock_data;

SELECT 'dwh.dim_customer' AS table_name, COUNT(*) AS rows_count FROM dwh.dim_customer
UNION ALL
SELECT 'dwh.dim_seller', COUNT(*) FROM dwh.dim_seller
UNION ALL
SELECT 'dwh.dim_product', COUNT(*) FROM dwh.dim_product
UNION ALL
SELECT 'dwh.dim_store', COUNT(*) FROM dwh.dim_store
UNION ALL
SELECT 'dwh.dim_supplier', COUNT(*) FROM dwh.dim_supplier
UNION ALL
SELECT 'dwh.dim_date', COUNT(*) FROM dwh.dim_date
UNION ALL
SELECT 'dwh.fact_sales', COUNT(*) FROM dwh.fact_sales;

SELECT
    p.product_category,
    ROUND(SUM(f.sale_total_price)::numeric, 2) AS revenue
FROM dwh.fact_sales f
JOIN dwh.dim_product p ON p.product_key = f.product_key
GROUP BY p.product_category
ORDER BY revenue DESC
LIMIT 10;
