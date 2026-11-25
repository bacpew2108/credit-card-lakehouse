SELECT
    k.card_brand,
    k.card_type,
    count(DISTINCT t.card_id) AS active_cards,
    count(*) AS txn_count,
    sum(t.amount) AS total_amount,
    round(100 * CAST(count(*) AS DOUBLE) / sum(count(*)) OVER (), 2) AS pct_txn,
    round(100 * CAST(sum(t.amount) AS DOUBLE) / sum(sum(t.amount)) OVER (), 2) AS pct_amount
FROM tx_ok AS t
JOIN cards AS k ON t.card_id = k.card_id
GROUP BY k.card_brand, k.card_type
ORDER BY k.card_brand, k.card_type
