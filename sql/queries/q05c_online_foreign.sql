-- Q5. Tỷ lệ giao dịch online và giao dịch ở nước ngoài theo năm.
-- Online theo vị trí (merchant_city = ONLINE), nước ngoài khi merchant_state không phải mã bang Mỹ.
-- Nguồn: gold.card.mart_geo_monthly. Dashboard D2.
-- khóa: year
SELECT
    year(month) AS year,
    sum(txn_count) AS txn_count,
    sum(txn_count) FILTER (WHERE is_online) AS online_count,
    round(100 * CAST(sum(txn_count) FILTER (WHERE is_online) AS double) / sum(txn_count), 2) AS pct_online,
    sum(txn_count) FILTER (WHERE is_foreign) AS foreign_count,
    round(100 * CAST(sum(txn_count) FILTER (WHERE is_foreign) AS double) / sum(txn_count), 3) AS pct_foreign
FROM gold.card.mart_geo_monthly
GROUP BY year(month)
ORDER BY year
