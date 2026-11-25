-- Q5. 20 thành phố trong nước có nhiều giao dịch nhất. Mart theo bang không có thành phố, nên
-- truy vấn này đi từ fact join dim_location.
-- Nguồn: gold.card.fact_transaction, gold.card.dim_location. Dashboard D2.
-- khóa: merchant_city, merchant_state
SELECT
    l.merchant_city,
    l.merchant_state,
    count(*) AS txn_count,
    sum(f.amount) AS total_amount
FROM gold.card.fact_transaction AS f
JOIN gold.card.dim_location AS l ON f.location_key = l.location_key
WHERE NOT l.is_online AND NOT l.is_foreign
GROUP BY l.merchant_city, l.merchant_state
ORDER BY txn_count DESC, l.merchant_city, l.merchant_state
LIMIT 20
