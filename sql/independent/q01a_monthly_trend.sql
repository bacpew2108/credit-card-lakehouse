WITH m AS (
    SELECT month, count(*) AS txn_count, sum(amount) AS total_amount FROM tx_ok GROUP BY month
)
SELECT
    month,
    year(month) AS year,
    txn_count,
    total_amount,
    round(CAST(total_amount AS DOUBLE) / txn_count, 2) AS avg_amount,
    round(CAST(total_amount AS DOUBLE) / avg(CAST(total_amount AS DOUBLE)) OVER (PARTITION BY year(month)), 4)
        AS seasonal_index
FROM m
ORDER BY month
