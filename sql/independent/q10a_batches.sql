-- ingested_rows = số dòng của file gốc trong tháng; quarantine_rows = số dòng vi phạm R01–R03;
-- fact_rows = số giao dịch hợp lệ. dq_warn, dq_block theo dq_month (_base.sql).
WITH raw AS (
    SELECT strftime(month, '%Y-%m') AS batch_id, count(*) AS ingested_rows FROM tx GROUP BY 1
),
q AS (
    SELECT batch_id, sum(failed) AS quarantine_rows FROM dq_month
    WHERE rule_id IN ('R01', 'R02', 'R03') GROUP BY batch_id
),
ok AS (
    SELECT strftime(month, '%Y-%m') AS batch_id, count(*) AS fact_rows FROM tx_ok GROUP BY 1
),
dq AS (
    SELECT batch_id, count_if(status = 'WARN') AS dq_warn, count_if(status = 'BLOCK') AS dq_block
    FROM dq_month GROUP BY batch_id
)
SELECT r.batch_id, r.ingested_rows, q.quarantine_rows, ok.fact_rows, dq.dq_warn, dq.dq_block
FROM raw AS r
JOIN q USING (batch_id)
JOIN ok USING (batch_id)
JOIN dq USING (batch_id)
ORDER BY r.batch_id
