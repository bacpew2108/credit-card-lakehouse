SELECT
    coalesce(g.mcc_group, 'Chưa phân nhóm') AS mcc_group,
    count(*) AS txn_count,
    sum(t.amount) AS total_amount,
    round(100 * CAST(sum(t.amount) AS DOUBLE) / sum(sum(t.amount)) OVER (), 2) AS pct_amount,
    round(CAST(sum(t.amount) AS DOUBLE) / count(*), 2) AS avg_amount
FROM tx_ok AS t
LEFT JOIN mcc_groups AS g ON t.mcc = g.mcc_code
GROUP BY 1
ORDER BY total_amount DESC
