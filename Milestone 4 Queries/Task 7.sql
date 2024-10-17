SELECT SUM(staff_numbers) AS total_staff_numbers, country_code
	FROM dim_store_details
		-- JOIN orders_table ON orders_table.date_uuid = dim_date_ti.date_uuid
		-- JOIN dim_products ON orders_table.product_code = dim_products.product_code
-- WHERE
GROUP BY country_code
-- HAVING
ORDER BY total_staff_numbers DESC
-- LIMIT
;