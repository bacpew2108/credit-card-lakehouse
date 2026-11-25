SELECT merchant_city, merchant_state, count(*) AS txn_count, sum(amount) AS total_amount
FROM tx_ok
WHERE NOT loc_online AND NOT loc_foreign
GROUP BY merchant_city, merchant_state
ORDER BY txn_count DESC, merchant_city, merchant_state
LIMIT 20
