-- Q4. Tỷ trọng giá trị và giá trị trung bình mỗi giao dịch theo nhóm ngành hàng (nhóm MCC).
-- Nguồn: gold.card.mart_mcc_monthly. Dashboard D2.
-- khóa: mcc_group
SELECT
    mcc_group,
    sum(txn_count) AS txn_count,
    sum(total_amount) AS total_amount,
    round(100 * CAST(sum(total_amount) AS double) / sum(sum(total_amount)) OVER (), 2) AS pct_amount,
    round(CAST(sum(total_amount) AS double) / sum(txn_count), 2) AS avg_amount
FROM gold.card.mart_mcc_monthly
GROUP BY mcc_group
ORDER BY total_amount DESC
