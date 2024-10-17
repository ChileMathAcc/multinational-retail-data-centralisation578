SELECT COUNT(orders_table.index) AS number_of_sales, SUM(product_quantity) as number_of_products,
	CASE
		WHEN locality IS NULL THEN 'Online'
		ELSE 'Offline'
	END AS store_location
FROM dim_store_details
	JOIN orders_table ON orders_table.store_code = dim_store_details.store_code
	-- JOIN dim_ ON orders_table. = dim_.
-- WHERE 
GROUP BY store_location
-- HAVING
-- ORDER BY
-- LIMIT
;