-- Q6. Tỷ lệ hoàn tiền (amount < 0) theo nhóm ngành hàng.
-- Nguồn: gold.card.mart_mcc_monthly. Dashboard D2.
-- khóa: mcc_group
SELECT
    mcc_group,
    sum(txn_count) AS txn_count,
    sum(refund_count) AS refund_count,
    round(100 * CAST(sum(refund_count) AS double) / sum(txn_count), 2) AS pct_refund,
    sum(refund_amount) AS refund_amount
FROM gold.card.mart_mcc_monthly
GROUP BY mcc_group
ORDER BY pct_refund DESC, mcc_group
