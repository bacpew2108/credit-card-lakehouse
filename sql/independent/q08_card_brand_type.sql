-- Thẻ của khách hàng đã xóa (card_id = −2) gộp vào nhóm Deleted, không tính là thẻ có giao dịch
SELECT
    coalesce(k.card_brand, 'Deleted') AS card_brand,
    coalesce(k.card_type, 'Deleted') AS card_type,
    count(DISTINCT t.card_id) FILTER (WHERE t.card_id <> -2) AS active_cards,
    count(*) AS txn_count,
    sum(t.amount) AS total_amount,
    round(100 * CAST(count(*) AS DOUBLE) / sum(count(*)) OVER (), 2) AS pct_txn,
    round(100 * CAST(sum(t.amount) AS DOUBLE) / sum(sum(t.amount)) OVER (), 2) AS pct_amount
FROM tx_ok AS t
LEFT JOIN cards AS k ON t.card_id = k.card_id
GROUP BY 1, 2
ORDER BY 1, 2
