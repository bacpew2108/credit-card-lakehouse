SELECT
    isodow(ts) AS day_of_week,
    ['Thứ Hai', 'Thứ Ba', 'Thứ Tư', 'Thứ Năm', 'Thứ Sáu', 'Thứ Bảy', 'Chủ Nhật'][isodow(ts)] AS day_name,
    hour(ts) AS txn_hour,
    count(*) AS txn_count,
    sum(amount) AS total_amount
FROM tx_ok
GROUP BY 1, 2, 3
ORDER BY day_of_week, txn_hour
