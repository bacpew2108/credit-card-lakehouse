"""Bảo trì bảng Iceberg (mục 8 và 11.1 Design Doc): mỗi lệnh chạy một thủ tục trên mọi bảng của bốn tầng.

    python iceberg_maintenance.py rewrite_data_files
    python iceberg_maintenance.py rewrite_manifests
    python iceberg_maintenance.py expire_snapshots [--retain-days 7] [--retain-last 3]
    python iceberg_maintenance.py remove_orphan_files [--orphan-days 3]
    python iceberg_maintenance.py all [--table ops.pipeline.dq_results]

- rewrite_data_files: gộp file nhỏ (binpack) trong từng partition. Bảng chỉ ghi thêm như dq_results,
  ingestion_log sinh một file mỗi lần ghi; bảng partition theo tháng thường chỉ có một file mỗi
  partition nên không có gì để gộp.
- rewrite_manifests: gộp manifest (mỗi commit thêm một manifest).
- expire_snapshots: xóa snapshot cũ hơn --retain-days ngày, luôn giữ --retain-last snapshot gần nhất,
  rồi xóa file dữ liệu và manifest không còn snapshot nào dùng. Sau bước này không time travel về
  các snapshot đã xóa được nữa.
- remove_orphan_files: xóa file trong thư mục bảng mà metadata không tham chiếu (job chết giữa
  chừng để lại), chỉ xét file cũ hơn --orphan-days ngày để không đụng vào job đang ghi.

Bảng nào có thay đổi thì in một dòng; bảng lỗi không dừng các bảng còn lại, cuối cùng job thoát với
mã lỗi nếu có bảng lỗi.
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone

from common import lakehouse as lh

def call(spark, table, procedure, **args):
    """CALL <catalog>.system.<procedure>(table => '<namespace>.<bảng>', …) → các dòng kết quả.
    Giá trị trong args là biểu thức SQL (VD "TIMESTAMP '2026-11-05 00:00:00'", "3", "true")."""
    catalog, ident = table.split(".", 1)
    sql_args = ", ".join(f"{k} => {v}" for k, v in {"table": f"'{ident}'", **args}.items())
    return spark.sql(f"CALL {catalog}.system.{procedure}({sql_args})").collect()


def cutoff(days, now=None):
    """Mốc thời gian 'cũ hơn days ngày' dạng literal SQL (session Spark dùng múi giờ UTC)."""
    t = (now or datetime.now(timezone.utc)) - timedelta(days=days)
    return f"TIMESTAMP '{t:%Y-%m-%d %H:%M:%S}'"


def rewrite_data_files(spark, table, args):
    r = call(spark, table, "rewrite_data_files")[0]
    if r.rewritten_data_files_count:
        return (f"gộp {r.rewritten_data_files_count} file thành {r.added_data_files_count} "
                f"({r.rewritten_bytes_count / 2**20:.1f} MiB)")


def rewrite_manifests(spark, table, args):
    r = call(spark, table, "rewrite_manifests")[0]
    if r.rewritten_manifests_count:
        return f"gộp {r.rewritten_manifests_count} manifest thành {r.added_manifests_count}"


def expire_snapshots(spark, table, args):
    before = spark.table(f"{table}.snapshots").count()
    r = call(spark, table, "expire_snapshots", older_than=cutoff(args.retain_days),
             retain_last=str(args.retain_last))[0]
    after = spark.table(f"{table}.snapshots").count()
    if after != before:
        deletes = r.deleted_position_delete_files_count + r.deleted_equality_delete_files_count
        return (f"{before} → {after} snapshot; xóa {r.deleted_data_files_count} file dữ liệu, "
                f"{deletes} file delete, {r.deleted_manifest_files_count} manifest, "
                f"{r.deleted_manifest_lists_count} manifest list")


def remove_orphan_files(spark, table, args):
    # prefix_listing: liệt kê qua S3FileIO của bảng (Lakekeeper ký request); mặc định thủ tục dùng
    # Hadoop FileSystem, mà image Spark không có hadoop-aws
    rows = call(spark, table, "remove_orphan_files", older_than=cutoff(args.orphan_days), prefix_listing="true")
    if rows:
        return f"xóa {len(rows)} file mồ côi"


# Theo thứ tự chạy của dag_maintenance và của lệnh all
RUN = {
    "rewrite_data_files": rewrite_data_files,
    "rewrite_manifests": rewrite_manifests,
    "expire_snapshots": expire_snapshots,
    "remove_orphan_files": remove_orphan_files,
}
ACTIONS = tuple(RUN)


def run(spark, action, tables, args):
    """Chạy một thủ tục trên các bảng → danh sách bảng lỗi."""
    t0, changed, failed = time.time(), 0, []
    for table in tables:
        t1 = time.time()
        try:
            note = RUN[action](spark, table, args)
        except Exception as e:
            failed.append(table)
            print(f"[maintenance] {action} {table}: LỖI {type(e).__name__}: {str(e).splitlines()[0][:300]}")
            continue
        if note:
            changed += 1
            print(f"[maintenance] {action} {table}: {note}; {time.time() - t1:.1f}s")
    print(f"{action}: {len(tables)} bảng, {changed} bảng có thay đổi, {len(failed)} bảng lỗi, {time.time() - t0:.0f}s")
    return failed


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=ACTIONS + ("all",))
    ap.add_argument("--table", action="append", help="chỉ bảo trì bảng này (lặp lại được); mặc định mọi bảng")
    ap.add_argument("--retain-days", type=float, default=7,
                    help="expire_snapshots: xóa snapshot cũ hơn số ngày này (mặc định %(default)s)")
    ap.add_argument("--retain-last", type=int, default=3,
                    help="expire_snapshots: luôn giữ ít nhất số snapshot gần nhất này (mặc định %(default)s)")
    ap.add_argument("--orphan-days", type=float, default=3,
                    help="remove_orphan_files: chỉ xóa file cũ hơn số ngày này, tối thiểu 1 (mặc định %(default)s)")
    args = ap.parse_args()
    if args.retain_days < 0 or args.retain_last < 1:
        ap.error("--retain-days phải ≥ 0 và --retain-last phải ≥ 1")
    if args.orphan_days < 1:
        ap.error("--orphan-days phải ≥ 1: Iceberg không cho xóa file mồ côi mới hơn 24 giờ")

    spark = lh.get_spark(f"maintenance_{args.action}")
    try:
        tables = args.table or lh.all_tables(spark)
        failed = {a: f for a in (ACTIONS if args.action == "all" else (args.action,))
                  if (f := run(spark, a, tables, args))}
    finally:
        spark.stop()
    if failed:
        sys.exit("[maintenance] lỗi: " + "; ".join(f"{a}: {', '.join(t)}" for a, t in failed.items()))


if __name__ == "__main__":
    sys.exit(main())
