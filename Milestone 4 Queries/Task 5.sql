SELECT MAX(SUM(product_quantity * product_price)) OVER (GROUP BY year) AS total_sales, year, month
FROM dim_date_times
	JOIN orders_table ON orders_table.date_uuid = dim_date_times.date_uuid
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
-- WHERE
GROUP BY year, month
-- HAVING
-- ORDER BY
-- LIMIT
;