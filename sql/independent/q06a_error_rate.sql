WITH e AS (
    SELECT channel, trim(unnest(string_split(errors, ','))) AS error_type
    FROM tx_ok WHERE errors IS NOT NULL AND errors <> ''
),
t AS (SELECT channel, count(*) AS channel_txn_count FROM tx_ok GROUP BY channel)
SELECT
    e.error_type,
    e.channel,
    count(*) AS error_count,
    t.channel_txn_count,
    round(100 * CAST(count(*) AS DOUBLE) / t.channel_txn_count, 3) AS pct_error
FROM e JOIN t ON e.channel = t.channel
GROUP BY e.error_type, e.channel, t.channel_txn_count
ORDER BY e.error_type, e.channel
