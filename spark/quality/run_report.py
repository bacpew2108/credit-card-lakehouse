"""Báo cáo lần chạy: ops.pipeline.pipeline_runs, một dòng cho mỗi lần chạy DAG (Hình 5 Design Doc).

    python run_report.py --month 2019-03 [--started-at 2026-11-12T08:00:00+00:00]
    python run_report.py --from 2010-01 --to 2018-12

Task publish_run_report (task cuối của dag_monthly, dag_rebuild) gọi job này khi mọi bước trước đã
thành công: ghi một dòng SUCCESS gồm số dòng của các tháng ở từng tầng, số bản ghi bị cách ly, số
rule PASS/WARN/BLOCK theo kết quả mới nhất và thời gian chạy. Đây là nguồn cho Q10 và dashboard D4.

Khi một task thất bại, on_failure_callback (airflow/dags/pipeline.py) gọi record_failure(): ghi một
dòng BLOCKED nếu chính lần chạy này ghi rule BLOCK vào dq_results, còn lại là FAILED, kèm tên task
và lý do. Chạy lại thành công thì có thêm dòng SUCCESS; dòng mới nhất của một run_id có hiệu lực.
"""

import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from common import helpers as h
from common import lakehouse as lh

MESSAGE_MAX = 1000


def dq_summary(rows):
    """rows: (rule_id, status, failed_count) là kết quả mới nhất của từng (batch, rule).
    → (số PASS, số WARN, số BLOCK, tóm tắt các rule không PASS như 'R07 WARN 5788; R12 WARN 1')."""
    counts, failed = {"PASS": 0, "WARN": 0, "BLOCK": 0}, {}
    for rule_id, status, n in rows:
        if status not in counts:            # INFO chỉ ghi nhận
            continue
        counts[status] += 1
        if status != "PASS":
            failed[rule_id, status] = failed.get((rule_id, status), 0) + (n or 0)
    issues = "; ".join(f"{rule} {status} {n}" for (rule, status), n in sorted(failed.items()))
    return counts["PASS"], counts["WARN"], counts["BLOCK"], issues or None


def _latest_dq(spark, months):
    return (spark.table(lh.DQ_TABLE).where(F.col("batch_id").isin(months))
                 .groupBy("batch_id", "rule_id")
                 .agg(F.max_by("status", "created_at").alias("status"),
                      F.max_by("failed_count", "created_at").alias("failed_count")))


def _count(spark, table, cond):
    return spark.table(table).where(cond).count() if spark.catalog.tableExists(table) else None


def collect(spark, months):
    """Số dòng từng tầng và tổng hợp DQ của các tháng → dict theo tên cột của pipeline_runs."""
    start, end = h.month_bounds(months[0])[0], h.month_bounds(months[-1])[1]
    ts = F.col("transaction_ts")
    in_range = (ts >= F.lit(start).cast("timestamp_ntz")) & (ts < F.lit(end).cast("timestamp_ntz"))
    in_batches = F.col("_batch_id").isin(months)
    source_rows = (spark.table(lh.LOG_TABLE)
                   .where((F.col("source") == h.SRC_TXN) & F.col("batch_id").isin(months))
                   .groupBy("batch_id").agg(F.max_by("source_count", "finished_at").alias("n"))
                   .agg(F.sum("n")).first()[0])
    dq = dq_summary([(r.rule_id, r.status, r.failed_count) for r in _latest_dq(spark, months).collect()])
    return {
        "source_rows": source_rows,
        "bronze_rows": _count(spark, h.BRONZE_TXN, in_batches),
        "silver_rows": _count(spark, h.SILVER_TXN, in_range),
        "quarantine_rows": _count(spark, h.QUARANTINE, in_batches),
        "fact_rows": _count(spark, h.GOLD_FACT, in_range),
        "dq_pass": dq[0], "dq_warn": dq[1], "dq_block": dq[2], "dq_issues": dq[3],
    }


def write(spark, run_id, dag_id, months, status, started_at, failed_task=None, message=None, **counts):
    """Thêm một dòng vào pipeline_runs. started_at None: không biết lúc bắt đầu, thời gian chạy là 0."""
    lh.ensure_ops_table(spark, lh.RUNS_TABLE, lh.RUNS_COLS)
    finished = datetime.now(timezone.utc)
    started = started_at or finished
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    row = {"run_id": run_id, "dag_id": dag_id, "batch_id": h.batch_label(months), "month_count": len(months),
           "status": status, "failed_task": failed_task, "message": message and message[:MESSAGE_MAX],
           "started_at": started, "finished_at": finished,
           "duration_seconds": round((finished - started).total_seconds(), 1), **counts}
    names = [c.split()[0] for c in lh.RUNS_COLS.split(",")]
    spark.createDataFrame([tuple(row.get(c) for c in names)], lh.RUNS_COLS).writeTo(lh.RUNS_TABLE).append()
    return row


def record_failure(run_id, dag_id, task_id, months, error, started_at=None):
    """Ghi dòng thất bại của một lần chạy (gọi từ on_failure_callback) → (status, message).

    BLOCKED khi chính lần chạy này (run_id) đã ghi rule BLOCK vào dq_results: message liệt kê rule
    và details. Còn lại là FAILED với thông báo lỗi của task.
    """
    spark = lh.get_spark("run_report_failure")
    try:
        blocks = (spark.table(lh.DQ_TABLE).where(F.col("run_id") == run_id)
                  .groupBy("batch_id", "rule_id")
                  .agg(F.max_by(F.struct("status", "failed_count", "checked_count", "details"), "created_at").alias("r"))
                  .where(F.col("r.status") == "BLOCK").orderBy("batch_id", "rule_id").collect())
        status = "BLOCKED" if blocks else "FAILED"
        message = "; ".join(f"{b.batch_id} {b.rule_id} BLOCK {b.r.failed_count}/{b.r.checked_count}"
                            + (f" ({b.r.details})" if b.r.details else "") for b in blocks) or error
        write(spark, run_id, dag_id, months, status, started_at, failed_task=task_id, message=message)
        return status, message
    finally:
        spark.stop()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    h.add_month_args(ap)
    ap.add_argument("--started-at", type=datetime.fromisoformat,
                    help="thời điểm lần chạy DAG bắt đầu (ISO 8601), để tính thời gian chạy")
    args = ap.parse_args()
    try:
        months = h.resolve_months(args)
    except ValueError as e:
        ap.error(str(e))

    spark = lh.get_spark("run_report")
    try:
        row = write(spark, lh.run_id(), os.environ.get("AIRFLOW_CTX_DAG_ID", "manual"), months, "SUCCESS",
                    args.started_at, **collect(spark, months))
    finally:
        spark.stop()
    print(f"[run] {row['dag_id']} {row['batch_id']} {row['status']}: nguồn {row['source_rows']}, "
          f"bronze {row['bronze_rows']}, silver {row['silver_rows']}, quarantine {row['quarantine_rows']}, "
          f"fact {row['fact_rows']}; DQ {row['dq_pass']} PASS, {row['dq_warn']} WARN, {row['dq_block']} BLOCK"
          + (f" ({row['dq_issues']})" if row["dq_issues"] else "") + f"; {row['duration_seconds']:.0f}s")


if __name__ == "__main__":
    sys.exit(main())
