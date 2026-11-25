WITH m AS (
    SELECT month, count(*) AS txn_count, CAST(sum(amount) AS DOUBLE) AS amt
    FROM tx_ok WHERE year(month) BETWEEN 2010 AND 2018 GROUP BY month
),
i AS (
    SELECT month, txn_count, amt / avg(amt) OVER (PARTITION BY year(month)) AS idx FROM m
)
SELECT
    month(month) AS month_of_year,
    round(avg(idx), 4) AS avg_index,
    round(min(idx), 4) AS min_index,
    round(max(idx), 4) AS max_index,
    round(avg(txn_count), 0) AS avg_txn_count
FROM i
GROUP BY month(month)
ORDER BY month_of_year
