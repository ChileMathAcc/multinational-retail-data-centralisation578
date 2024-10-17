SELECT SUM(product_quantity * product_price) AS total_sales, store_type, dim_store_details.country_code
	FROM dim_store_details
		JOIN orders_table ON orders_table.store_code = dim_store_details.store_code
		JOIN dim_products ON orders_table.product_code = dim_products.product_code
WHERE country_code = 'DE'
GROUP BY country_code, store_type
-- HAVING
ORDER BY total_sales 
-- LIMIT
;