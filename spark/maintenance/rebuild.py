"""Các bước riêng của dag_rebuild: dựng lại Silver và Gold từ Bronze rồi chứng minh kết quả trùng khớp
(mục 8, 11.1 và tiêu chí "tính tái lập" ở mục 13 Design Doc).

    python rebuild.py snapshot
    python rebuild.py clean --from 2015-01 --to 2015-12
    python rebuild.py verify --from 2015-01 --to 2015-12 --before '{"silver.card.transactions": 123, …}'

snapshot: in id snapshot hiện tại của silver.card.transactions, gold.card.fact_transaction và
gold.card.mart_monthly_kpi thành một dòng JSON (dòng cuối của stdout, BashOperator đẩy vào XCom).
clean: xóa các tháng khỏi Silver giao dịch, fact và 6 mart, để bước sau dựng lại từ con số 0. Chỉ
xóa khi mọi tháng đều có Bronze nạp thành công, vì Bronze là nguồn duy nhất để dựng lại.
verify: với từng tháng, so bảng hiện tại với chính nó ở snapshot đã ghi lúc đầu (time travel): số
dòng, tổng amount và checksum mọi cột nghiệp vụ. Lệch số dòng hoặc tổng tiền thì job thoát với mã
lỗi. Chỉ lệch checksum thì cảnh báo: số liệu tổng không đổi nhưng nội dung dòng đã khác, thường do
dữ liệu tham chiếu hoặc logic biến đổi thay đổi từ lần dựng trước. Tháng chưa có ở snapshot cũ
(nạp lần đầu) không có gì để so.
"""

import argparse
import json
import sys

from pyspark.sql import functions as F

from common import helpers as h
from common import lakehouse as lh

# bảng: (cột xác định tháng, biểu thức số dòng, biểu thức tổng tiền)
TABLES = {
    h.SILVER_TXN: ("transaction_ts", "count(*)", "sum(amount)"),
    h.GOLD_FACT: ("transaction_ts", "count(*)", "sum(amount)"),
    h.MARTS["monthly_kpi"]: ("month", "sum(txn_count)", "sum(total_amount)"),
}


def compare(before, after, months):
    """before, after: {tháng: (số dòng, tổng tiền, checksum)} của một bảng trước và sau khi dựng lại.
    → {tháng: 'same' | 'content' (chỉ lệch checksum) | 'diff' (lệch số dòng, tổng tiền) | 'new'}."""
    out = {}
    for m in months:
        b, a = before.get(m), after.get(m)
        if b is None:
            out[m] = "new"
        elif a is None or b[:2] != a[:2]:
            out[m] = "diff"
        else:
            out[m] = "same" if b[2] == a[2] else "content"
    return out


def _in_range(col, months):
    """Điều kiện SQL: cột tháng (timestamp_ntz hoặc date) nằm trong các tháng đã cho."""
    start, end = h.month_bounds(months[0])[0], h.month_bounds(months[-1])[1]
    if col == "month":
        return f"month >= DATE '{start}' AND month < DATE '{end}'"
    return f"{col} >= TIMESTAMP_NTZ '{start} 00:00:00' AND {col} < TIMESTAMP_NTZ '{end} 00:00:00'"


def current_snapshot(spark, table):
    """Id snapshot hiện tại của bảng; None nếu bảng chưa có hoặc chưa có snapshot."""
    if not spark.catalog.tableExists(table):
        return None
    row = spark.table(f"{table}.refs").where(F.col("name") == "main").first()
    return row.snapshot_id if row else None


def totals(df, table, months):
    """{tháng: (số dòng, tổng tiền, checksum)} của df (bảng table ở một snapshot) trong các tháng."""
    month_col, n, amount = TABLES[table]
    cols = [c for c in df.columns if not c.startswith("_")]     # bỏ _batch_id, _updated_at
    rows = (df.where(_in_range(month_col, months))
              .groupBy(F.date_format(month_col, "yyyy-MM").alias("m"))
              .agg(F.expr(n).alias("n"), F.expr(amount).alias("amount"),
                   F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("checksum")).collect())
    return {r.m: (r.n, r.amount, r.checksum) for r in rows}


def clean(spark, months):
    statuses = lh.latest_status(spark, h.SRC_TXN)
    for m in months:
        lh.require_bronze(spark, h.SRC_TXN, m, statuses)
    targets = [(h.SILVER_TXN, "transaction_ts"), (h.GOLD_FACT, "transaction_ts")]
    targets += [(t, "month") for t in h.MARTS.values()]
    for table, col in targets:
        if not spark.catalog.tableExists(table):
            continue
        cond = _in_range(col, months)
        n = spark.table(table).where(cond).count()
        if n:
            spark.sql(f"DELETE FROM {table} WHERE {cond}")
        print(f"[rebuild] {table}: xóa {n} dòng của {months[0]} → {months[-1]}")


def verify(spark, months, before_ids):
    """So từng bảng với snapshot trước khi dựng lại → số tháng lệch số dòng hoặc tổng tiền."""
    bad = 0
    for table in TABLES:
        sid = before_ids.get(table)
        if sid is None:
            print(f"[rebuild] {table}: chưa có snapshot trước khi dựng lại (nạp lần đầu), không có gì để so")
            continue
        before = totals(spark.read.option("snapshot-id", sid).table(table), table, months)
        result = compare(before, totals(spark.table(table), table, months), months)
        by = {k: [m for m, v in result.items() if v == k] for k in ("same", "content", "diff", "new")}
        note = f"{len(by['same'])}/{len(months)} tháng trùng khớp số dòng, tổng tiền và checksum"
        if by["content"]:
            note += f"; {len(by['content'])} tháng khớp số dòng, tổng tiền nhưng lệch checksum ({', '.join(by['content'][:6])})"
        if by["new"]:
            note += f"; {len(by['new'])} tháng chưa có trước khi dựng lại"
        if by["diff"]:
            note += f"; LỆCH số dòng hoặc tổng tiền ở {len(by['diff'])} tháng ({', '.join(by['diff'][:6])})"
        print(f"[rebuild] {table} so với snapshot {sid}: {note}")
        bad += len(by["diff"])
    return bad


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("snapshot", help="in id snapshot hiện tại của Silver giao dịch, fact, mart_monthly_kpi")
    h.add_month_args(sub.add_parser("clean", help="xóa các tháng khỏi Silver giao dịch, fact, mart"))
    v = sub.add_parser("verify", help="so kết quả dựng lại với snapshot trước đó")
    h.add_month_args(v)
    v.add_argument("--before", required=True, type=json.loads, help="JSON do lệnh snapshot in ra")
    args = ap.parse_args()
    months = None
    if args.cmd != "snapshot":
        try:
            months = h.resolve_months(args)
        except ValueError as e:
            ap.error(str(e))

    spark = lh.get_spark(f"rebuild_{args.cmd}")
    try:
        if args.cmd == "snapshot":
            out = json.dumps({t: current_snapshot(spark, t) for t in TABLES})
        elif args.cmd == "clean":
            clean(spark, months)
        else:
            bad = verify(spark, months, args.before)
    finally:
        spark.stop()
    if args.cmd == "snapshot":
        print(out)
    elif args.cmd == "verify" and bad:
        sys.exit(f"[rebuild] dựng lại KHÔNG trùng khớp: {bad} cặp (bảng, tháng) lệch số dòng hoặc tổng tiền")


if __name__ == "__main__":
    sys.exit(main())
