SELECT
    coalesce(g.mcc_group, 'Chưa phân nhóm') AS mcc_group,
    count(*) AS txn_count,
    count_if(t.amount < 0) AS refund_count,
    round(100 * CAST(count_if(t.amount < 0) AS DOUBLE) / count(*), 2) AS pct_refund,
    coalesce(sum(t.amount) FILTER (WHERE t.amount < 0), 0) AS refund_amount
FROM tx_ok AS t
LEFT JOIN mcc_groups AS g ON t.mcc = g.mcc_code
GROUP BY 1
ORDER BY pct_refund DESC, mcc_group
