"""Quyền xóa dữ liệu (mục 7, 11.1 Design Doc): xóa một khách hàng khỏi Bronze, Silver, Gold (make erase).

    python erase_customer.py --customer-id 1510
    python erase_customer.py --customer-id 1510 --dry-run    # chỉ đếm số dòng sẽ bị sửa

Minh họa kỹ thuật cho quyền xóa dữ liệu (Luật Bảo vệ dữ liệu cá nhân số 91/2025/QH15), không phải
đánh giá tuân thủ pháp lý. Các bước:

1. Ghi yêu cầu vào ops.privacy.erasure_log (STARTED) trước khi sửa dữ liệu. extract_jdbc đọc danh
   sách này ở mỗi lần trích xuất, nên nạp lại từ nguồn (dag_reference, dag_rebuild có extract) không
   đưa khách hàng trở lại.
2. Xóa dòng của khách hàng và thẻ: bronze customers_raw, cards_raw; silver customers, cards; gold
   dim_customer, dim_card.
3. Giữ giao dịch nhưng ẩn danh, để số dòng và tổng tiền không đổi: client_id, card_id → '-2' ở bronze
   transactions_raw và ops quarantine_transactions; customer_id, card_id → −2 ở silver transactions;
   fact: customer_key, card_key → −2 (Deleted), customer_age_at_txn → NULL.
4. Dựng lại 6 mart cho các tháng có giao dịch của khách (mart_card_monthly có từng thẻ, mart phân
   khúc có tuổi, thu nhập của khách), rồi chạy lại rule trên dữ liệu đã sửa: R04–R08, R12, R14 và R11
   cho các tháng đó, R09 cho bảng tham chiếu. Kết quả mới nhất trong dq_results nhờ vậy khớp dữ liệu
   hiện tại (VD giao dịch ngoài thời hạn thẻ của khách không còn tính là R14, vì thẻ đã bị xóa).
5. Xóa lịch sử: expire_snapshots mọi bảng đã sửa, chỉ giữ snapshot hiện tại. File dữ liệu cũ (còn dòng
   của khách) bị xóa khỏi MinIO, time travel về trước lúc xóa không còn được. Iceberg không xóa được
   lịch sử của riêng một khách: mọi snapshot cũ của các bảng này đều mất. remove_orphan_files dọn
   file mồ côi cũ hơn 1 ngày.
6. Kiểm tra: không bảng nào còn dòng của khách; snapshot cũ không đọc được; file cũ không còn trên
   MinIO (đọc bằng tài khoản chỉ đọc pan-scanner, không qua catalog). Đạt thì ghi DONE, không thì
   FAILED và thoát mã lỗi.

Không chạy cùng lúc với job khác ghi vào các bảng này: Iceberg chỉ cho một commit thắng, job còn lại
lỗi. Dòng cuối stdout là JSON (snapshot cũ, số file cũ) cho make demo.
"""

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from common import helpers as h
from common import lakehouse as lh
from maintenance import iceberg_maintenance as im
from maintenance import rebuild
from quality import reconcile
from quality import rules as dq
from transform import marts

E = lh.ERASED_ID


def ids_sql(ids, quote=False):
    """Danh sách id cho IN (…); rỗng → NULL (IN (NULL) không khớp dòng nào)."""
    return ", ".join(f"'{int(i)}'" if quote else str(int(i)) for i in sorted(ids)) or "NULL"


def plan(cid, card_ids, customer_keys, card_keys):
    """Các thao tác theo thứ tự tầng: [(bảng, 'DELETE' | 'UPDATE', điều kiện, SET hoặc None)].

    Điều kiện chọn đúng các dòng của khách hàng; chạy xong thì điều kiện không còn khớp dòng nào
    (dòng bị xóa, hoặc id đã thành −2), nên dùng lại để kiểm tra.
    """
    raw_hit = f"client_id = '{int(cid)}' OR card_id IN ({ids_sql(card_ids, quote=True)})"
    hit = f"customer_id = {int(cid)} OR card_id IN ({ids_sql(card_ids)})"
    return [
        (h.BRONZE_CUSTOMERS, "DELETE", f"id = '{int(cid)}'", None),
        (h.BRONZE_CARDS, "DELETE", f"client_id = '{int(cid)}'", None),
        (h.BRONZE_TXN, "UPDATE", raw_hit, f"client_id = '{E}', card_id = '{E}'"),
        (h.QUARANTINE, "UPDATE", raw_hit, f"client_id = '{E}', card_id = '{E}'"),
        (h.SILVER_CUSTOMERS, "DELETE", f"customer_id = {int(cid)}", None),
        (h.SILVER_CARDS, "DELETE", f"customer_id = {int(cid)}", None),
        (h.SILVER_TXN, "UPDATE", hit, f"customer_id = {E}, card_id = {E}"),
        (h.GOLD_DIMS["customer"], "DELETE", f"customer_id = {int(cid)}", None),
        (h.GOLD_DIMS["card"], "DELETE", f"customer_id = {int(cid)}", None),
        (h.GOLD_FACT, "UPDATE",
         f"customer_key IN ({ids_sql(customer_keys)}) OR card_key IN ({ids_sql(card_keys)})",
         f"customer_key = {E}, card_key = {E}, customer_age_at_txn = NULL"),
    ]


def find(spark, cid):
    """Thẻ và khóa của khách hàng ở mọi tầng (Bronze có thể còn thẻ mà Silver đã bỏ)."""
    card_ids = {r.card_id for r in spark.table(h.SILVER_CARDS).where(F.col("customer_id") == cid)
                .select("card_id").collect()}
    card_ids |= {int(r.id) for r in spark.table(h.BRONZE_CARDS).where(F.col("client_id") == str(cid))
                 .select("id").collect()}
    customer_keys = {r.customer_key for r in spark.table(h.GOLD_DIMS["customer"])
                     .where(F.col("customer_id") == cid).collect()}
    card_keys = {r.card_key for r in spark.table(h.GOLD_DIMS["card"])
                 .where(F.col("card_id").isin(list(card_ids)) | (F.col("customer_id") == cid)).collect()}
    return card_ids, customer_keys, card_keys


def affected_months(spark, hit):
    """Các tháng có giao dịch của khách hàng trong Silver (trước khi ẩn danh)."""
    row = (spark.table(h.SILVER_TXN).where(hit)
           .agg(F.date_format(F.min("transaction_ts"), "yyyy-MM").alias("a"),
                F.date_format(F.max("transaction_ts"), "yyyy-MM").alias("b")).first())
    return h.iter_months(row.a, row.b) if row.a else []


def old_files(spark, table, cond):
    """Đường dẫn các file dữ liệu đang chứa dòng của khách hàng (cột metadata _file của Iceberg)."""
    return sorted(r._file for r in spark.table(table).where(cond).select("_file").distinct().collect())


def log(spark, rid, cid, card_ids, status, details, started):
    lh.ensure_ops_table(spark, lh.ERASURE_TABLE, lh.ERASURE_COLS)
    row = (rid, int(cid), sorted(int(c) for c in card_ids), status, json.dumps(details, ensure_ascii=False),
           started, datetime.now(timezone.utc))
    spark.createDataFrame([row], lh.ERASURE_COLS).writeTo(lh.ERASURE_TABLE).append()


def expire(spark, table):
    """Chỉ giữ snapshot hiện tại → (số snapshot trước, sau, số file dữ liệu đã xóa)."""
    before = spark.table(f"{table}.snapshots").count()
    now = datetime.now(timezone.utc) + timedelta(seconds=1)
    r = im.call(spark, table, "expire_snapshots", older_than=im.cutoff(0, now), retain_last="1")[0]
    return before, spark.table(f"{table}.snapshots").count(), r.deleted_data_files_count


def time_travel_fails(spark, table, snapshot_id):
    try:
        spark.read.option("snapshot-id", snapshot_id).table(table).limit(1).count()
        return False
    except Exception:  # noqa: BLE001 – snapshot đã bị expire: Iceberg báo không tìm thấy
        return True


def missing_on_minio(paths):
    """Số file trong paths không còn trên MinIO (head_object 404), đọc bằng tài khoản pan-scanner."""
    from botocore.exceptions import ClientError

    from quality import pan_scan
    s3, gone = pan_scan.s3_client(), 0
    for p in paths:
        bucket, key = p.removeprefix("s3://").split("/", 1)
        try:
            s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            gone += e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound")
    return gone


def erase(spark, cid, dry_run=False):
    started, t0 = datetime.now(timezone.utc), time.time()
    rid = f"erase_{int(cid)}_{started:%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
    card_ids, customer_keys, card_keys = find(spark, cid)
    steps = plan(cid, card_ids, customer_keys, card_keys)
    counts = {t: spark.table(t).where(cond).count() for t, _, cond, _ in steps}
    months = affected_months(spark, steps[6][2])
    print(f"[erase] khách hàng {cid}: {len(card_ids)} thẻ {sorted(card_ids)}, giao dịch trong "
          f"{len(months)} tháng ({h.batch_label(months) or 'không có'})")
    for t, op, _, _ in steps:
        print(f"[erase]   {op:6} {t}: {counts[t]} dòng")
    if not any(counts.values()):
        done = cid in lh.erased_customers(spark)[0]
        print(f"[erase] không còn dữ liệu của khách hàng {cid}" + (" (đã xóa trước đó)" if done else ""))
        return 0 if done else 1
    if dry_run:
        return 0

    touched = [t for t, *_ in steps if counts[t]] + list(h.MARTS.values())
    before = {t: rebuild.current_snapshot(spark, t) for t in touched}
    files = {t: old_files(spark, t, cond) for t, _, cond, _ in steps if counts[t]}
    files[h.MARTS["card_monthly"]] = old_files(spark, h.MARTS["card_monthly"],
                                               f"card_key IN ({ids_sql(card_keys)})")
    log(spark, rid, cid, card_ids, "STARTED", {"rows": counts}, started)

    for t, op, cond, set_ in steps:
        if counts[t]:
            t1 = time.time()
            spark.sql(f"DELETE FROM {t} WHERE {cond}" if op == "DELETE" else f"UPDATE {t} SET {set_} WHERE {cond}")
            print(f"[erase] {op} {t}: {counts[t]} dòng, {time.time() - t1:.1f}s")
    if months:
        marts.run(spark, months)
        blocked = dq.run_transactions(spark, rid, months, None)
        bad = reconcile.silver_gold(spark, rid, months)
        print(f"[erase] R04–R08, R12, R14, R11 {h.batch_label(months)}: "
              + (f"BLOCK {blocked}, R11 lệch {bad}" if blocked or bad else "không có BLOCK"))
    dq.run_reference(spark, rid, None)

    expired = {}
    for t in touched:
        expired[t] = expire(spark, t)
        im.call(spark, t, "remove_orphan_files", older_than=im.cutoff(1), prefix_listing="true")
        b, a, n = expired[t]
        print(f"[erase] expire {t}: {b} → {a} snapshot, xóa {n} file dữ liệu")

    remaining = {t: spark.table(t).where(cond).count() for t, _, cond, _ in steps}
    remaining[h.MARTS["card_monthly"]] = spark.table(h.MARTS["card_monthly"]).where(
        F.col("card_key").isin(list(card_keys))).count()
    travel = {t: time_travel_fails(spark, t, s) for t, s in before.items() if s}
    all_files = [p for ps in files.values() for p in ps]
    gone = missing_on_minio(all_files)
    ok = not any(remaining.values()) and all(travel.values()) and gone == len(all_files)
    details = {"rows": counts, "months": h.batch_label(months), "remaining": sum(remaining.values()),
               "old_snapshots_unreadable": f"{sum(travel.values())}/{len(travel)}",
               "old_files_gone": f"{gone}/{len(all_files)}"}
    log(spark, rid, cid, card_ids, "DONE" if ok else "FAILED", details, started)
    print(f"[erase] còn {sum(remaining.values())} dòng của khách hàng; snapshot cũ không đọc được "
          f"{sum(travel.values())}/{len(travel)} bảng; file cũ đã xóa khỏi MinIO {gone}/{len(all_files)}")
    print(f"[erase] {'DONE' if ok else 'FAILED'} {rid}, {time.time() - t0:.0f}s")
    print(json.dumps({"request_id": rid, "customer_id": int(cid), "status": "DONE" if ok else "FAILED",
                      "old_snapshots": {t: s for t, s in before.items() if s},
                      "old_files": len(all_files), "old_files_gone": gone}))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--customer-id", type=int, required=True)
    ap.add_argument("--dry-run", action="store_true", help="chỉ đếm số dòng sẽ bị sửa, không ghi gì")
    args = ap.parse_args()
    if args.customer_id <= 0:
        ap.error("--customer-id phải là số nguyên dương")

    spark = lh.get_spark("erase_customer")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS ops.privacy")
        return erase(spark, args.customer_id, args.dry_run)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
