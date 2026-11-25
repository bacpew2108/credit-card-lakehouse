-- Q8. Đóng góp của thương hiệu thẻ và loại thẻ vào số giao dịch và giá trị.
-- Nguồn: gold.card.mart_card_monthly. Dashboard D3.
-- khóa: card_brand, card_type
SELECT
    card_brand,
    card_type,
    count(DISTINCT card_key) AS active_cards,
    sum(txn_count) AS txn_count,
    sum(total_amount) AS total_amount,
    round(100 * CAST(sum(txn_count) AS double) / sum(sum(txn_count)) OVER (), 2) AS pct_txn,
    round(100 * CAST(sum(total_amount) AS double) / sum(sum(total_amount)) OVER (), 2) AS pct_amount
FROM gold.card.mart_card_monthly
GROUP BY card_brand, card_type
ORDER BY card_brand, card_type
