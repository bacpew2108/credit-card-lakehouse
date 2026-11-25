-- Q5. Phân bố giao dịch trong nước theo bang nơi giao dịch diễn ra (không gồm online, nước ngoài).
-- Theo dim_location (vị trí của từng giao dịch), không theo vị trí chính của merchant.
-- Nguồn: gold.card.mart_geo_monthly. Dashboard D2.
-- khóa: merchant_state
SELECT
    merchant_state,
    sum(txn_count) AS txn_count,
    sum(total_amount) AS total_amount,
    round(100 * CAST(sum(txn_count) AS double) / sum(sum(txn_count)) OVER (), 2) AS pct_txn
FROM gold.card.mart_geo_monthly
WHERE NOT is_online AND NOT is_foreign
GROUP BY merchant_state
ORDER BY txn_count DESC
