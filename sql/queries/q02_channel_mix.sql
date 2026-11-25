-- Q2. Cơ cấu kênh giao dịch (quẹt thẻ, chip, online) qua các năm, theo số giao dịch.
-- Nguồn: gold.card.mart_monthly_kpi (swipe_count, chip_count, online_count). Dashboard D1.
-- khóa: year, channel
SELECT
    year(k.month) AS year,
    c.channel,
    sum(c.n) AS txn_count,
    round(100 * CAST(sum(c.n) AS double) / sum(sum(c.n)) OVER (PARTITION BY year(k.month)), 2) AS pct_txn
FROM gold.card.mart_monthly_kpi AS k
CROSS JOIN UNNEST(ARRAY['Swipe', 'Chip', 'Online'],
                  ARRAY[k.swipe_count, k.chip_count, k.online_count]) AS c (channel, n)
GROUP BY year(k.month), c.channel
ORDER BY year, channel
