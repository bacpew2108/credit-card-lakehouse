-- Nhóm tuổi theo tuổi tại thời điểm giao dịch (năm giao dịch − năm sinh); thu nhập, điểm tín dụng
-- theo ngưỡng ở docs/data_model.md mục 3 "Nhóm phân khúc"
WITH b AS (
    SELECT
        t.client_id,
        t.month,
        t.amount,
        CASE
            WHEN year(t.ts) - c.birth_year < 18 THEN '< 18'
            WHEN year(t.ts) - c.birth_year < 25 THEN '18–24'
            WHEN year(t.ts) - c.birth_year < 35 THEN '25–34'
            WHEN year(t.ts) - c.birth_year < 45 THEN '35–44'
            WHEN year(t.ts) - c.birth_year < 55 THEN '45–54'
            WHEN year(t.ts) - c.birth_year < 65 THEN '55–64'
            ELSE '65+'
        END AS age_band,
        CASE
            WHEN c.yearly_income < 30000 THEN '< 30k'
            WHEN c.yearly_income < 40000 THEN '30–40k'
            WHEN c.yearly_income < 50000 THEN '40–50k'
            WHEN c.yearly_income < 75000 THEN '50–75k'
            ELSE '≥ 75k'
        END AS income_band,
        CASE
            WHEN c.credit_score < 580 THEN 'Poor (< 580)'
            WHEN c.credit_score < 670 THEN 'Fair (580–669)'
            WHEN c.credit_score < 740 THEN 'Good (670–739)'
            WHEN c.credit_score < 800 THEN 'Very Good (740–799)'
            ELSE 'Exceptional (≥ 800)'
        END AS credit_score_band
    FROM tx_ok AS t
    JOIN customers AS c ON t.client_id = c.customer_id
),
s AS (
    SELECT 'age_band' AS segment_type, age_band AS segment, client_id, month, amount FROM b
    UNION ALL SELECT 'income_band', income_band, client_id, month, amount FROM b
    UNION ALL SELECT 'credit_score_band', credit_score_band, client_id, month, amount FROM b
)
SELECT
    segment_type,
    segment,
    count(DISTINCT (client_id, month)) AS customer_months,
    count(*) AS txn_count,
    sum(amount) AS total_amount,
    round(CAST(sum(amount) AS DOUBLE) / count(DISTINCT (client_id, month)), 2) AS avg_spend_per_customer_month
FROM s
GROUP BY segment_type, segment
ORDER BY segment_type, segment
