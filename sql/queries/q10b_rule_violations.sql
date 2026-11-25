-- Q10. Rule nào vi phạm nhiều nhất: trên các batch tháng, theo kết quả mới nhất của từng (tháng, rule).
-- violations là tổng failed_count (R12: số tháng bất thường; R10, R11: số cặp đối soát lệch).
-- Nguồn: ops.pipeline.dq_results. Dashboard D4.
-- khóa: rule_id
WITH latest AS (
    SELECT
        batch_id,
        rule_id,
        max_by(severity, created_at) AS severity,
        max_by(status, created_at) AS status,
        max_by(failed_count, created_at) AS failed_count
    FROM ops.pipeline.dq_results
    WHERE regexp_like(batch_id, '^\d{4}-\d{2}$')
    GROUP BY batch_id, rule_id
)
SELECT
    rule_id,
    severity,
    count(*) AS months,
    count_if(failed_count > 0) AS months_violated,
    sum(failed_count) AS violations,
    count_if(status = 'BLOCK') AS months_block
FROM latest
WHERE severity <> 'INFO'
GROUP BY rule_id, severity
ORDER BY violations DESC, rule_id
