WITH monthly_sales AS (
	SELECT SUM(product_quantity * product_price) AS total_sales, year, month
		FROM dim_date_times
			JOIN orders_table ON orders_table.date_uuid = dim_date_times.date_uuid
			JOIN dim_products ON orders_table.product_code = dim_products.product_code
-- WHERE
GROUP BY year, month)
SELECT total_sales, year, month
	FROM monthly_sales
		WHERE (year, total_sales) IN (
			SELECT year, MAX(total_sales)
				FROM monthly_sales
			GROUP BY year
		)
-- HAVING
-- ORDER BY
-- LIMIT
;