SELECT SUM(product_price * product_quantity) AS total_sale, month
FROM dim_date_times
	JOIN orders_table ON orders_table.date_uuid = dim_date_times.date_uuid
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
-- WHERE
GROUP BY month
-- HAVING
-- ORDER BY 
-- LIMIT
;