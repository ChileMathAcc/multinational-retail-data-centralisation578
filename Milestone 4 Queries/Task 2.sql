SELECT locality, count(store_code)
FROM dim_store_details
WHERE store_type <> 'Web Portal'
GROUP BY locality
-- HAVING
ORDER BY count(store_code) DESC
-- LIMIT
;