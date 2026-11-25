WITH m AS (
    SELECT
        t.mcc AS mcc_code,
        d.description,
        coalesce(g.mcc_group, 'Chưa phân nhóm') AS mcc_group,
        count(*) AS txn_count,
        sum(t.amount) AS total_amount,
        CAST(sum(t.amount) AS DOUBLE) / sum(sum(t.amount)) OVER () AS share
    FROM tx_ok AS t
    LEFT JOIN mcc AS d ON t.mcc = d.mcc_code
    LEFT JOIN mcc_groups AS g ON t.mcc = g.mcc_code
    GROUP BY 1, 2, 3
)
SELECT
    mcc_code, description, mcc_group, txn_count, total_amount,
    round(100 * share, 2) AS pct_amount,
    round(CAST(total_amount AS DOUBLE) / txn_count, 2) AS avg_amount
FROM m
ORDER BY total_amount DESC, mcc_code
LIMIT 15
