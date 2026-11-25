-- Q9. Phân bố mức sử dụng hạn mức của thẻ tín dụng: chi tiêu tháng (tổng giao dịch mua, amount > 0)
-- / credit_limit, trên mỗi cặp (thẻ, tháng) có giao dịch. Chỉ thẻ Credit có credit_limit > 0
-- (31 thẻ có hạn mức 0 bị loại). Tính lại từ purchase_amount, credit_limit thay vì cột utilization
-- (đã làm tròn 4 chữ số) để nhóm ở biên không lệch.
-- Nguồn: gold.card.mart_card_monthly. Dashboard D3.
-- khóa: bucket_order
WITH u AS (
    SELECT CAST(purchase_amount AS double) / CAST(credit_limit AS double) AS util
    FROM gold.card.mart_card_monthly
    WHERE card_type = 'Credit' AND credit_limit > 0
),
b AS (
    SELECT
        CASE WHEN util < 0.1 THEN 1 WHEN util < 0.3 THEN 2 WHEN util < 0.5 THEN 3
             WHEN util < 0.8 THEN 4 WHEN util < 1 THEN 5 ELSE 6 END AS bucket_order,
        util
    FROM u
)
SELECT
    bucket_order,
    element_at(ARRAY['< 10%', '10–30%', '30–50%', '50–80%', '80–100%', '≥ 100%'], bucket_order) AS bucket,
    count(*) AS card_months,
    round(100 * CAST(count(*) AS double) / sum(count(*)) OVER (), 2) AS pct_card_months,
    round(100 * avg(util), 2) AS avg_util_pct
FROM b
GROUP BY bucket_order
ORDER BY bucket_order
