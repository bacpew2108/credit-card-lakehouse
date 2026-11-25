SELECT
    merchant_state,
    count(*) AS txn_count,
    sum(amount) AS total_amount,
    round(100 * CAST(count(*) AS DOUBLE) / sum(count(*)) OVER (), 2) AS pct_txn
FROM tx_ok
WHERE NOT loc_online AND NOT loc_foreign
GROUP BY merchant_state
ORDER BY txn_count DESC
