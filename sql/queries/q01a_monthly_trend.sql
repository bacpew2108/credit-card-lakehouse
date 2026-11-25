-- Q1. Số giao dịch, tổng giá trị, giá trị trung bình theo tháng; tính mùa vụ.
-- seasonal_index = tổng giá trị của tháng / trung bình tháng của cùng năm (1 là bằng trung bình năm).
-- Nguồn: gold.card.mart_monthly_kpi. Dashboard D1.
-- khóa: month
SELECT
    month,
    year(month) AS year,
    txn_count,
    total_amount,
    round(CAST(total_amount AS double) / txn_count, 2) AS avg_amount,
    round(CAST(total_amount AS double)
          / avg(CAST(total_amount AS double)) OVER (PARTITION BY year(month)), 4) AS seasonal_index
FROM gold.card.mart_monthly_kpi
ORDER BY month
