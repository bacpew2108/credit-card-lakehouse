-- Q1. Tính mùa vụ: chỉ số mùa vụ trung bình của từng tháng trong năm, trên 9 năm đủ 12 tháng (2010–2018).
-- Chỉ số > 1 là tháng thường cao hơn trung bình năm. min/max cho thấy độ ổn định qua các năm.
-- Nguồn: gold.card.mart_monthly_kpi. Dashboard D1.
-- khóa: month_of_year
WITH m AS (
    SELECT
        month,
        txn_count,
        CAST(total_amount AS double)
            / avg(CAST(total_amount AS double)) OVER (PARTITION BY year(month)) AS idx
    FROM gold.card.mart_monthly_kpi
    WHERE year(month) BETWEEN 2010 AND 2018
)
SELECT
    month(month) AS month_of_year,
    round(avg(idx), 4) AS avg_index,
    round(min(idx), 4) AS min_index,
    round(max(idx), 4) AS max_index,
    round(avg(txn_count), 0) AS avg_txn_count
FROM m
GROUP BY month(month)
ORDER BY month_of_year
