-- Q10. Mỗi batch tháng: số dòng nạp vào Bronze (lần nạp SUCCESS gần nhất), số bản ghi bị cách ly,
-- số dòng fact, số rule WARN/BLOCK theo kết quả mới nhất của từng rule (không tính rule INFO).
-- Nguồn: ops.pipeline.ingestion_log, quarantine_transactions, dq_results; gold.card.mart_monthly_kpi. Dashboard D4.
-- khóa: batch_id
WITH ingested AS (
    SELECT batch_id, max_by(row_count, finished_at) AS ingested_rows
    FROM ops.pipeline.ingestion_log
    WHERE source = 'core_card.transactions' AND status = 'SUCCESS'
    GROUP BY batch_id
),
quarantined AS (
    SELECT _batch_id AS batch_id, count(*) AS quarantine_rows
    FROM ops.pipeline.quarantine_transactions
    GROUP BY _batch_id
),
latest AS (
    SELECT batch_id, rule_id, max_by(severity, created_at) AS severity, max_by(status, created_at) AS status
    FROM ops.pipeline.dq_results
    WHERE regexp_like(batch_id, '^\d{4}-\d{2}$')
    GROUP BY batch_id, rule_id
),
dq AS (
    SELECT batch_id, count_if(status = 'WARN') AS dq_warn, count_if(status = 'BLOCK') AS dq_block
    FROM latest
    WHERE severity <> 'INFO'
    GROUP BY batch_id
)
SELECT
    i.batch_id,
    i.ingested_rows,
    coalesce(q.quarantine_rows, 0) AS quarantine_rows,
    k.txn_count AS fact_rows,
    d.dq_warn,
    d.dq_block
FROM ingested AS i
LEFT JOIN quarantined AS q ON i.batch_id = q.batch_id
LEFT JOIN gold.card.mart_monthly_kpi AS k ON i.batch_id = format_datetime(k.month, 'yyyy-MM')
LEFT JOIN dq AS d ON i.batch_id = d.batch_id
ORDER BY i.batch_id
