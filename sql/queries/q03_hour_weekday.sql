-- Q3. Giao dịch theo giờ trong ngày × ngày trong tuần (giờ địa phương của giao dịch).
-- Truy vấn trực tiếp trên fact (star join với dim_date), không có mart.
-- Nguồn: gold.card.fact_transaction, gold.card.dim_date. Dashboard D1.
-- khóa: day_of_week, txn_hour
SELECT
    d.day_of_week,
    d.day_name,
    f.txn_hour,
    count(*) AS txn_count,
    sum(f.amount) AS total_amount
FROM gold.card.fact_transaction AS f
JOIN gold.card.dim_date AS d ON f.date_key = d.date_key
GROUP BY d.day_of_week, d.day_name, f.txn_hour
ORDER BY d.day_of_week, f.txn_hour
