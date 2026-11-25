-- Q10. Pipeline chạy mất bao lâu: số lần chạy và thời gian theo DAG và kết quả.
-- Nguồn: ops.pipeline.pipeline_runs (dòng mới nhất của mỗi lần chạy). Dashboard D4.
-- khóa: dag_id, status
WITH runs AS (
    SELECT
        run_id,
        max_by(dag_id, finished_at) AS dag_id,
        max_by(status, finished_at) AS status,
        max_by(month_count, finished_at) AS month_count,
        max_by(duration_seconds, finished_at) AS duration_seconds
    FROM ops.pipeline.pipeline_runs
    GROUP BY run_id
)
SELECT
    dag_id,
    status,
    count(*) AS runs,
    sum(month_count) AS months,
    round(avg(duration_seconds), 0) AS avg_seconds,
    round(max(duration_seconds), 0) AS max_seconds,
    round(sum(duration_seconds) / nullif(sum(month_count), 0), 1) AS seconds_per_month
FROM runs
GROUP BY dag_id, status
ORDER BY dag_id, status
