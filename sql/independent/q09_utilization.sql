WITH cm AS (
    SELECT
        t.card_id,
        t.month,
        k.credit_limit,
        coalesce(sum(t.amount) FILTER (WHERE t.amount > 0), 0) AS purchase
    FROM tx_ok AS t
    JOIN cards AS k ON t.card_id = k.card_id
    WHERE k.card_type = 'Credit' AND k.credit_limit > 0
    GROUP BY t.card_id, t.month, k.credit_limit
),
b AS (
    SELECT
        CAST(purchase AS DOUBLE) / CAST(credit_limit AS DOUBLE) AS util,
        CASE
            WHEN CAST(purchase AS DOUBLE) / CAST(credit_limit AS DOUBLE) < 0.1 THEN 1
            WHEN CAST(purchase AS DOUBLE) / CAST(credit_limit AS DOUBLE) < 0.3 THEN 2
            WHEN CAST(purchase AS DOUBLE) / CAST(credit_limit AS DOUBLE) < 0.5 THEN 3
            WHEN CAST(purchase AS DOUBLE) / CAST(credit_limit AS DOUBLE) < 0.8 THEN 4
            WHEN CAST(purchase AS DOUBLE) / CAST(credit_limit AS DOUBLE) < 1 THEN 5
            ELSE 6
        END AS bucket_order
    FROM cm
)
SELECT
    bucket_order,
    ['< 10%', '10–30%', '30–50%', '50–80%', '80–100%', '≥ 100%'][bucket_order] AS bucket,
    count(*) AS card_months,
    round(100 * CAST(count(*) AS DOUBLE) / sum(count(*)) OVER (), 2) AS pct_card_months,
    round(100 * avg(util), 2) AS avg_util_pct
FROM b
GROUP BY bucket_order
ORDER BY bucket_order
