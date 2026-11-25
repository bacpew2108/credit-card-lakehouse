-- Q6. Tỷ lệ giao dịch lỗi theo loại lỗi và kênh. Một giao dịch có nhiều lỗi được tính ở mỗi loại.
-- Mẫu số là số giao dịch của kênh (từ mart_monthly_kpi, đủ mọi tháng kể cả tháng kênh đó không có lỗi).
-- Nguồn: gold.card.mart_error_monthly, gold.card.mart_monthly_kpi. Dashboard D2.
-- khóa: error_type, channel
WITH channel_total AS (
    SELECT c.channel, sum(c.n) AS channel_txn_count
    FROM gold.card.mart_monthly_kpi AS k
    CROSS JOIN UNNEST(ARRAY['Swipe', 'Chip', 'Online'],
                      ARRAY[k.swipe_count, k.chip_count, k.online_count]) AS c (channel, n)
    GROUP BY c.channel
)
SELECT
    e.error_type,
    e.channel,
    sum(e.error_count) AS error_count,
    t.channel_txn_count,
    round(100 * CAST(sum(e.error_count) AS double) / t.channel_txn_count, 3) AS pct_error
FROM gold.card.mart_error_monthly AS e
JOIN channel_total AS t ON e.channel = t.channel
GROUP BY e.error_type, e.channel, t.channel_txn_count
ORDER BY e.error_type, e.channel
