"""Đối soát giữa các tầng (R10, R11, mục 6 Design Doc), ghi kết quả vào ops.pipeline.dq_results.

    python reconcile.py src-bronze --month 2019-03
    python reconcile.py src-bronze --from 2010-01 --to 2019-10
    python reconcile.py bronze-silver --month 2019-03
    python reconcile.py silver-gold --month 2019-03

src-bronze (R10.1): đếm lại giao dịch tháng M ở nguồn và ở Bronze ngay lúc chạy, nên vẫn kiểm tra
được khi extract bỏ qua tháng đã nạp thành công.
bronze-silver (R10.2): bronze = silver + quarantine của tháng M, kèm số bản ghi bị cách ly theo
R01–R03. silver_transactions.py gọi bronze_silver() sau mỗi tháng.
silver-gold (R11): số dòng và tổng amount của tháng M ở Silver = fact = mart_monthly_kpi.
Có tháng lệch thì job ghi BLOCK rồi thoát với mã lỗi.
"""

import argparse
import sys
import time

from pyspark.sql import functions as F

from common import helpers as h
from common import lakehouse as lh
from quality import rules as dq

QUARANTINE_RULES = ("R01", "R02", "R03")


def src_bronze(spark, rid, month):
    src_n = lh.count_jdbc(spark, "transactions", h.month_filter(month))
    bronze_n = spark.table(h.BRONZE_TXN).where(F.col("_batch_id") == month).count()
    sign = "=" if src_n == bronze_n else "≠"
    status = dq.record(spark, rid, month, [("R10.1", src_n, abs(src_n - bronze_n),
                                            f"nguồn {src_n} {sign} bronze {bronze_n}")])
    return status["R10.1"]


def bronze_silver(spark, rid, month, block_ratio=None, bronze_n=None):
    """Ghi R01–R03 (số bản ghi bị cách ly, theo lý do) và R10.2 của tháng M.
    → (silver_n, quarantine_n, {rule_id: status})."""
    in_month = F.col("_batch_id") == month
    if bronze_n is None:
        bronze_n = spark.table(h.BRONZE_TXN).where(in_month).count()
    silver_n = spark.table(h.SILVER_TXN).where(h.month_scope(month).format(t="")).count()
    by_reason = spark.table(h.QUARANTINE).where(in_month).groupBy("rule_id", "reason").count().collect()
    results = []
    for rule_id in QUARANTINE_RULES:
        reasons = sorted((r.reason, r["count"]) for r in by_reason if r.rule_id == rule_id)
        failed = sum(n for _, n in reasons)
        results.append((rule_id, bronze_n, failed, "; ".join(f"{m}: {n}" for m, n in reasons) or None))
    q_n = sum(r["count"] for r in by_reason)
    sign = "=" if bronze_n == silver_n + q_n else "≠"
    results.append(("R10.2", bronze_n, abs(bronze_n - silver_n - q_n),
                    f"bronze {bronze_n} {sign} silver {silver_n} + quarantine {q_n}"))
    return silver_n, q_n, dq.record(spark, rid, month, results, block_ratio)


def r11_compare(silver, fact, mart):
    """(số dòng, tổng amount) của tháng ở Silver, fact, mart_monthly_kpi (None: không có dữ liệu)
    → (số cặp lệch trong 4 cặp so sánh, details)."""
    s, f, m = (x or (0, 0) for x in (silver, fact, mart))
    pairs = (s[0] == f[0], s[1] == f[1], f[0] == m[0], f[1] == m[1])
    details = f"silver {s[0]} dòng, {s[1]}; fact {f[0]} dòng, {f[1]}; mart {m[0]} dòng, {m[1]}"
    return pairs.count(False), details


def _totals(df, month_col):
    """{tháng: (số dòng, tổng amount)} của df theo cột month_col."""
    rows = (df.groupBy(F.date_format(month_col, "yyyy-MM").alias("m"))
              .agg(F.count(F.lit(1)).alias("n"), F.sum("amount").alias("amount")).collect())
    return {r.m: (r.n, r.amount or 0) for r in rows}


def silver_gold(spark, rid, months):
    """Ghi R11 cho các tháng (tổng hợp mỗi bảng một lần cho cả khoảng); trả về các tháng lệch."""
    start, end = h.month_bounds(months[0])[0], h.month_bounds(months[-1])[1]
    ts = F.col("transaction_ts")
    in_range = (ts >= F.lit(start).cast("timestamp_ntz")) & (ts < F.lit(end).cast("timestamp_ntz"))
    silver = _totals(spark.table(h.SILVER_TXN).where(in_range), "transaction_ts")
    fact = _totals(spark.table(h.GOLD_FACT).where(in_range), "transaction_ts")
    mart = {r.m: (r.txn_count, r.total_amount) for r in
            spark.table(h.MARTS["monthly_kpi"])
                 .where((F.col("month") >= F.lit(start).cast("date")) & (F.col("month") < F.lit(end).cast("date")))
                 .select(F.date_format("month", "yyyy-MM").alias("m"), "txn_count", "total_amount").collect()}
    bad = []
    for m in months:
        failed, details = r11_compare(silver.get(m), fact.get(m), mart.get(m))
        if dq.record(spark, rid, m, [("R11", 4, failed, details)])["R11"] == "BLOCK":
            bad.append(m)
    return bad


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, desc in (("src-bronze", "R10.1: số dòng nguồn = bronze"),
                       ("bronze-silver", "R10.2: bronze = silver + quarantine, kèm R01–R03"),
                       ("silver-gold", "R11: silver = fact = mart_monthly_kpi")):
        p = sub.add_parser(name, help=desc)
        h.add_month_args(p)
        if name == "bronze-silver":
            dq.add_block_ratio_arg(p)
    args = ap.parse_args()
    try:
        months = h.resolve_months(args)
    except ValueError as e:
        ap.error(str(e))

    spark = lh.get_spark(f"reconcile_{args.cmd.replace('-', '_')}")
    t0 = time.time()
    try:
        rid = lh.run_id()
        if args.cmd == "src-bronze":
            bad = [m for m in months if src_bronze(spark, rid, m) == "BLOCK"]
        elif args.cmd == "bronze-silver":
            bad = [m for m in months if bronze_silver(spark, rid, m, args.block_ratio)[2]["R10.2"] == "BLOCK"]
        else:
            bad = silver_gold(spark, rid, months)
    finally:
        spark.stop()
    print(f"Đối soát {args.cmd} {len(months)} tháng ({months[0]} → {months[-1]}): "
          f"{len(bad)} tháng lệch, {time.time() - t0:.0f}s")
    if bad:
        sys.exit(f"[BLOCK] {'R11' if args.cmd == 'silver-gold' else 'R10'} lệch ở {', '.join(bad)}")


if __name__ == "__main__":
    sys.exit(main())
