-- Q4. 15 ngành hàng (MCC) có tổng giá trị lớn nhất, kèm tỷ trọng và giá trị trung bình mỗi giao dịch.
-- Nguồn: gold.card.mart_mcc_monthly. Dashboard D2.
-- khóa: mcc_code
WITH m AS (
    SELECT
        mcc_code,
        description,
        mcc_group,
        sum(txn_count) AS txn_count,
        sum(total_amount) AS total_amount,
        CAST(sum(total_amount) AS double) / sum(sum(total_amount)) OVER () AS share
    FROM gold.card.mart_mcc_monthly
    GROUP BY mcc_code, description, mcc_group
)
SELECT
    mcc_code,
    description,
    mcc_group,
    txn_count,
    total_amount,
    round(100 * share, 2) AS pct_amount,
    round(CAST(total_amount AS double) / txn_count, 2) AS avg_amount
FROM m
ORDER BY total_amount DESC, mcc_code
LIMIT 15
