-- Đủ mọi tổ hợp năm × kênh: năm chưa có giao dịch Chip (trước 2015) vẫn có dòng với số 0
WITH n AS (
    SELECT year(month) AS year, channel, count(*) AS txn_count FROM tx_ok GROUP BY 1, 2
),
grid AS (
    SELECT y.year, c.channel
    FROM (SELECT DISTINCT year(month) AS year FROM tx_ok) AS y
    CROSS JOIN (VALUES ('Swipe'), ('Chip'), ('Online')) AS c (channel)
)
SELECT
    g.year,
    g.channel,
    coalesce(n.txn_count, 0) AS txn_count,
    round(100 * CAST(coalesce(n.txn_count, 0) AS DOUBLE)
          / sum(coalesce(n.txn_count, 0)) OVER (PARTITION BY g.year), 2) AS pct_txn
FROM grid AS g
LEFT JOIN n ON g.year = n.year AND g.channel = n.channel
ORDER BY g.year, g.channel
