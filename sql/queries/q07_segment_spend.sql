-- Q7. Chi tiêu trung bình mỗi khách mỗi tháng theo nhóm tuổi (tại thời điểm giao dịch), nhóm thu nhập,
-- nhóm điểm tín dụng. customer_months = số cặp (khách, tháng) có giao dịch; mỗi khách mỗi tháng chỉ
-- thuộc một ô của mart, nên cộng customer_count qua các ô không đếm trùng.
-- Nguồn: gold.card.mart_customer_segment_monthly. Dashboard D3.
-- khóa: segment_type, segment
WITH s AS (
    SELECT 'age_band' AS segment_type, age_band AS segment, customer_count, txn_count, total_amount
    FROM gold.card.mart_customer_segment_monthly
    UNION ALL
    SELECT 'income_band', income_band, customer_count, txn_count, total_amount
    FROM gold.card.mart_customer_segment_monthly
    UNION ALL
    SELECT 'credit_score_band', credit_score_band, customer_count, txn_count, total_amount
    FROM gold.card.mart_customer_segment_monthly
)
SELECT
    segment_type,
    segment,
    sum(customer_count) AS customer_months,
    sum(txn_count) AS txn_count,
    sum(total_amount) AS total_amount,
    round(CAST(sum(total_amount) AS double) / sum(customer_count), 2) AS avg_spend_per_customer_month
FROM s
GROUP BY segment_type, segment
ORDER BY segment_type, segment
