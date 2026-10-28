"""Rule chất lượng dữ liệu và ops.pipeline.dq_results (mục 6 Design Doc).

    python rules.py transactions --month 2019-03 [--block-ratio 0.01]
    python rules.py transactions --from 2010-01 --to 2019-10
    python rules.py reference

transactions: với mỗi tháng M, chạy R04–R08, R14 và R12 trên silver.card.transactions của M, ghi
dq_results, rồi xét kết quả mới nhất của các rule từ nguồn đến Silver của M (SILVER_RULES: gồm
R01–R03, R10 do job Silver và reconcile.py ghi; không gồm R11 của Gold, để một lần R11 BLOCK cũ
không chặn việc dựng lại Gold). Có rule BLOCK thì job thoát với mã lỗi, task dq_silver thất bại
và DAG dừng.
reference: R09 trên silver.card.customers và silver.card.cards, batch là ref_… của khách hàng.

Trạng thái (status) của một rule trong một batch:
- PASS: không có vi phạm.
- WARN: có vi phạm nhưng batch đi tiếp. Với R01–R03, bản ghi vi phạm đã bị cách ly.
- BLOCK: dừng DAG. Rule đối soát (R10, R11) lệch, hoặc rule xét từng bản ghi có tỷ lệ vi phạm vượt
  ngưỡng (--block-ratio, mặc định DQ_BLOCK_RATIO hoặc 1%).
- INFO: chỉ ghi nhận (I01, merchant nhiều vị trí), không bao giờ chặn.
"""

import argparse
import math
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from functools import reduce
from operator import or_
from typing import NamedTuple

from pyspark.sql import functions as F

from common import helpers as h
from common import lakehouse as lh


class Rule(NamedTuple):
    severity: str       # BLOCK | WARN | INFO
    per_record: bool    # True: xét từng bản ghi, tỷ lệ vi phạm vượt ngưỡng thì BLOCK
    description: str


RULES = {
    "R01": Rule("BLOCK", True, "transaction_id không null, không trùng"),
    "R02": Rule("BLOCK", True, "amount, date parse được"),
    "R03": Rule("BLOCK", True, "transaction_ts thuộc đúng tháng của batch"),
    "R04": Rule("WARN", True, "client_id có trong customers; card_id có trong cards"),
    "R05": Rule("WARN", True, "thẻ thuộc đúng khách hàng"),
    "R06": Rule("WARN", True, "mcc có trong mcc_codes"),
    "R07": Rule("WARN", True, "channel ∈ {Swipe, Chip, Online}; Online ⇔ merchant_city = ONLINE"),
    "R08": Rule("WARN", True, "thẻ has_chip = NO không có giao dịch Chip"),
    "R09": Rule("WARN", True, "credit_score ∈ [300, 850]; num_credit_cards khớp số thẻ thực tế"),
    "R10.1": Rule("BLOCK", False, "số dòng nguồn = bronze"),
    "R10.2": Rule("BLOCK", False, "bronze = silver + quarantine"),
    "R11": Rule("BLOCK", False, "số dòng, tổng amount theo tháng: silver = fact = mart_monthly_kpi"),
    "R12": Rule("WARN", False, "số giao dịch và tổng tiền của tháng trong ±3σ so với 12 tháng trước"),
    "R14": Rule("WARN", True, "giao dịch nằm trong thời hạn thẻ"),
    "I01": Rule("INFO", False, "merchant có nhiều vị trí (city, state, zip)"),
}
# Rule từ nguồn đến Silver: dq_silver và Gold chỉ cho đi tiếp khi không rule nào trong số này BLOCK (mục 4.1)
SILVER_RULES = tuple(r for r in RULES if r not in ("R11", "I01"))
CHANNELS = ("Swipe", "Chip", "Online")
DEFAULT_BLOCK_RATIO = 0.01


# ---------------------------------------------------------------- trạng thái, ghi kết quả

def default_block_ratio():
    return float(os.environ.get("DQ_BLOCK_RATIO") or DEFAULT_BLOCK_RATIO)


def add_block_ratio_arg(ap):
    ap.add_argument("--block-ratio", type=float, default=default_block_ratio(),
                    help="tỷ lệ vi phạm của rule xét từng bản ghi mà vượt quá thì BLOCK (mặc định %(default)s)")


def decide(rule_id, checked, failed, block_ratio=None):
    """→ (failed_ratio, status) theo quy ước ở đầu file. failed_ratio là None khi không kiểm tra dòng nào."""
    rule = RULES[rule_id]
    ratio = failed / checked if checked else None
    if rule.severity == "INFO":
        status = "INFO"
    elif not failed:
        status = "PASS"
    elif not rule.per_record:
        status = rule.severity
    else:
        limit = default_block_ratio() if block_ratio is None else block_ratio
        status = "BLOCK" if ratio > limit else "WARN"
    return ratio, status


def record(spark, rid, batch_id, results, block_ratio=None):
    """Ghi kết quả rule của một batch vào dq_results. results: [(rule_id, checked, failed, details)].
    In một dòng tóm tắt, trả về {rule_id: status}."""
    now = datetime.now(timezone.utc)
    rows, notes = [], []
    for rule_id, checked, failed, details in results:
        ratio, status = decide(rule_id, checked, failed, block_ratio)
        rows.append((rid, batch_id, rule_id, RULES[rule_id].severity, checked, failed, ratio, status, details, now))
        if status != "PASS":
            notes.append(f"{rule_id} {status} {failed}/{checked}" + (f" ({details})" if details else ""))
    spark.createDataFrame(rows, lh.DQ_COLS).writeTo(lh.DQ_TABLE).append()
    n_pass = len(rows) - len(notes)
    print(f"[dq] {batch_id}: " + "; ".join(notes + [f"{n_pass} rule PASS"] if n_pass else notes))
    return {r[2]: r[7] for r in rows}


def blocked_batches(spark, batch_ids, rules=None):
    """{batch: [rule]} các batch có rule mà kết quả mới nhất là BLOCK; rules giới hạn các rule xét."""
    df = spark.table(lh.DQ_TABLE).where(F.col("batch_id").isin(list(batch_ids)))
    if rules is not None:
        df = df.where(F.col("rule_id").isin(list(rules)))
    rows = (df.groupBy("batch_id", "rule_id").agg(F.max_by("status", "created_at").alias("status"))
              .where(F.col("status") == "BLOCK").orderBy("batch_id", "rule_id").collect())
    out = {}
    for r in rows:
        out.setdefault(r.batch_id, []).append(r.rule_id)
    return out


def blocked_rules(spark, batch_id, rules=None):
    """Các rule mà kết quả mới nhất của batch là BLOCK."""
    return blocked_batches(spark, [batch_id], rules).get(batch_id, [])


# ---------------------------------------------------------------- rule xét từng bản ghi

def flag(df, key, checks):
    """checks: {rule_id: [(nhãn, điều kiện)]} → key + một cột boolean cho từng điều kiện (R07_1, …)
    và một cột cho từng rule (OR các điều kiện). Điều kiện NULL (thiếu dữ liệu để xét) là không vi phạm."""
    cols = [key]
    for rule_id, parts in checks.items():
        conds = [F.coalesce(cond, F.lit(False)) for _, cond in parts]
        cols += [c.alias(f"{rule_id}_{i}") for i, c in enumerate(conds, 1)]
        cols.append(reduce(or_, conds).alias(rule_id))
    return df.select(*cols)


def summarize(flags, checks):
    """Đếm vi phạm của từng rule và từng điều kiện trong một lượt → [(rule_id, checked, failed, details)].
    checks chỉ dùng để lấy tên rule và nhãn điều kiện; details liệt kê số vi phạm theo điều kiện khi
    rule có nhiều điều kiện."""
    names = [c for rule_id, parts in checks.items()
             for c in [rule_id] + [f"{rule_id}_{i}" for i in range(1, len(parts) + 1)]]
    row = flags.agg(F.count(F.lit(1)).alias("_n"), *[F.count_if(F.col(c)).alias(c) for c in names]).first()
    out = []
    for rule_id, parts in checks.items():
        failed, details = row[rule_id], None
        if failed and len(parts) > 1:
            details = "; ".join(f"{label}: {row[f'{rule_id}_{i}']}" for i, (label, _) in enumerate(parts, 1))
        out.append((rule_id, row["_n"], failed, details))
    return out


def txn_checks():
    """Điều kiện vi phạm R04–R08, R14 trên kết quả của join_reference()."""
    day = F.to_date("transaction_ts")
    city_online = F.coalesce(F.col("merchant_city") == "ONLINE", F.lit(False))
    return {
        "R04": [("client_id không có trong customers", F.col("_customer").isNull()),
                ("card_id không có trong cards", F.col("_card").isNull())],
        "R05": [("thẻ thuộc khách hàng khác",
                 F.col("_card").isNotNull() & ~F.col("_card_customer").eqNullSafe(F.col("customer_id")))],
        "R06": [("mcc không có trong mcc_codes", F.col("_mcc").isNull())],
        "R07": [("kênh lạ", ~F.coalesce(F.col("channel").isin(*CHANNELS), F.lit(False))),
                ("Online nhưng merchant_city khác ONLINE", (F.col("channel") == "Online") & ~city_online),
                ("không phải Online nhưng merchant_city = ONLINE", (F.col("channel") != "Online") & city_online)],
        "R08": [("thẻ không có chip nhưng giao dịch Chip", (F.col("channel") == "Chip") & ~F.col("_has_chip"))],
        # acct_open_date là ngày đầu tháng mở thẻ, expires_date là ngày cuối tháng hết hạn
        "R14": [("trước tháng mở thẻ", day < F.col("_open")),
                ("sau tháng hết hạn", day > F.col("_expires"))],
    }


def join_reference(txn, customers, cards, mcc_codes):
    """Giao dịch Silver + thông tin tham chiếu cần cho txn_checks() (broadcast: bảng tham chiếu nhỏ)."""
    cust = customers.select("customer_id", F.lit(True).alias("_customer"))
    card = cards.select("card_id", F.lit(True).alias("_card"), F.col("customer_id").alias("_card_customer"),
                        F.col("has_chip").alias("_has_chip"), F.col("acct_open_date").alias("_open"),
                        F.col("expires_date").alias("_expires"))
    mcc = mcc_codes.select("mcc_code", F.lit(True).alias("_mcc"))
    return (txn.join(F.broadcast(cust), "customer_id", "left")
               .join(F.broadcast(card), "card_id", "left")
               .join(F.broadcast(mcc), "mcc_code", "left"))


def txn_flags(txn, customers, cards, mcc_codes):
    """Cờ R04–R08, R14 cho từng giao dịch Silver (transaction_id + cột boolean, xem flag())."""
    return flag(join_reference(txn, customers, cards, mcc_codes), "transaction_id", txn_checks())


def customer_checks():
    """Điều kiện vi phạm R09 trên silver.card.customers đã join số thẻ thực tế (_n_cards)."""
    return {"R09": [("credit_score ngoài [300, 850]",
                     ~F.coalesce(F.col("credit_score").between(300, 850), F.lit(False))),
                    ("num_credit_cards khác số thẻ thực tế",
                     ~F.col("num_credit_cards").eqNullSafe(F.coalesce(F.col("_n_cards"), F.lit(0))))]}


def customer_flags(customers, cards):
    n_cards = cards.groupBy("customer_id").agg(F.count(F.lit(1)).alias("_n_cards"))
    return flag(customers.join(n_cards, "customer_id", "left"), "customer_id", customer_checks())


# ---------------------------------------------------------------- R12

def r12(history, month):
    """history: {tháng: (số giao dịch, tổng tiền)} gồm tháng M và các tháng trước nó.
    → (checked, failed, details). Mỗi tháng là một lần kiểm tra; vi phạm khi một trong hai chỉ số
    lệch khỏi trung bình 12 tháng liền trước quá 3 độ lệch chuẩn (mẫu). Chưa đủ 12 tháng thì không xét."""
    prev = [history.get(m) for m in h.iter_months(h.add_months(month, -12), h.add_months(month, -1))]
    have = sum(p is not None for p in prev)
    if have < 12 or month not in history:
        return 0, 0, f"chưa đủ 12 tháng trước ({have}/12), không xét"
    failed, notes = 0, []
    for i, name in enumerate(("số giao dịch", "tổng tiền")):
        xs = [float(p[i]) for p in prev]
        mu, sd, x = statistics.mean(xs), statistics.stdev(xs), float(history[month][i])
        if abs(x - mu) > 3 * sd:
            failed = 1
        z = (x - mu) / sd if sd else (0.0 if x == mu else math.copysign(math.inf, x - mu))
        notes.append(f"{name} z = {z:+.2f}")
    return 1, failed, "; ".join(notes)


def monthly_totals(spark, first, last):
    """{tháng: (số giao dịch, tổng tiền)} của silver.card.transactions từ first đến last."""
    start, end = h.month_bounds(first)[0], h.month_bounds(last)[1]
    ts = F.col("transaction_ts")
    rows = (spark.table(h.SILVER_TXN)
            .where((ts >= F.lit(start).cast("timestamp_ntz")) & (ts < F.lit(end).cast("timestamp_ntz")))
            .groupBy(F.date_format(ts, "yyyy-MM").alias("month"))
            .agg(F.count(F.lit(1)).alias("n"), F.sum("amount").alias("amount"))
            .collect())
    return {r.month: (r.n, r.amount) for r in rows}


# ---------------------------------------------------------------- job

def run_transactions(spark, rid, months, block_ratio):
    """Rule WARN cho từng tháng rồi xét chặn; trả về {tháng: [rule BLOCK]}."""
    ref = [spark.table(t).persist() for t in (h.SILVER_CUSTOMERS, h.SILVER_CARDS, h.SILVER_MCC)]
    history = monthly_totals(spark, h.add_months(months[0], -12), months[-1])
    blocked = {}
    for month in months:
        txn = spark.table(h.SILVER_TXN).where(h.month_scope(month).format(t=""))
        results = summarize(txn_flags(txn, *ref), txn_checks())
        results.append(("R12", *r12(history, month)))
        record(spark, rid, month, results, block_ratio)
        if b := blocked_rules(spark, month, SILVER_RULES):
            blocked[month] = b
    for df in ref:
        df.unpersist()
    return blocked


def run_reference(spark, rid, block_ratio):
    customers, cards = spark.table(h.SILVER_CUSTOMERS), spark.table(h.SILVER_CARDS)
    # Batch là lần nạp Bronze khách hàng gần nhất mà Silver vừa đọc (MERGE không đổi _batch_id
    # của dòng không thay đổi, nên max(_batch_id) của Silver có thể là batch cũ hơn)
    batch_id = lh.require_bronze(spark, h.SRC_CUSTOMERS)
    record(spark, rid, batch_id, summarize(customer_flags(customers, cards), customer_checks()), block_ratio)
    b = blocked_rules(spark, batch_id)
    return {batch_id: b} if b else {}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("transactions", help="R04–R08, R12, R14 theo tháng, rồi xét chặn")
    h.add_month_args(t)
    add_block_ratio_arg(t)
    r = sub.add_parser("reference", help="R09 trên khách hàng và thẻ")
    add_block_ratio_arg(r)
    args = ap.parse_args()
    try:
        months = h.resolve_months(args) if args.cmd == "transactions" else None
    except ValueError as e:
        ap.error(str(e))

    spark = lh.get_spark(f"dq_{args.cmd}")
    t0 = time.time()
    try:
        rid = lh.run_id()
        if args.cmd == "transactions":
            blocked = run_transactions(spark, rid, months, args.block_ratio)
            scope = f"{len(months)} tháng ({months[0]} → {months[-1]})"
        else:
            blocked = run_reference(spark, rid, args.block_ratio)
            scope = "bảng tham chiếu"
    finally:
        spark.stop()
    print(f"DQ {scope}: {len(blocked)} batch bị chặn, {time.time() - t0:.0f}s")
    if blocked:
        sys.exit("[BLOCK] " + "; ".join(f"{b}: {', '.join(r)}" for b, r in blocked.items()))


if __name__ == "__main__":
    sys.exit(main())
