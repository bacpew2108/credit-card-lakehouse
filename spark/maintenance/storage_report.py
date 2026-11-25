"""Dung lượng dữ liệu đang dùng của từng bảng, từng tầng (mục 8 và 11 Design Doc).

    python storage_report.py [--record]

Cộng file_size_in_bytes của các file dữ liệu thuộc snapshot hiện tại (bảng metadata .files),
nên không tính snapshot cũ, file mồ côi hay metadata. Dung lượng vật lý của bucket (mc du trong
make storage-status) lớn hơn số này đúng bằng phần chờ expire_snapshots / remove_orphan_files dọn.

--record ghi thêm mỗi bảng một dòng vào ops.pipeline.table_stats (task table_stats của
dag_maintenance), để theo dõi số file, số snapshot và dung lượng qua các lần bảo trì.
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import functions as F

from common import lakehouse as lh


def measure(spark):
    """[(bảng, số dòng, byte dữ liệu, số file, số snapshot)] của mọi bảng, theo thứ tự tầng."""
    out = []
    for name in lh.all_tables(spark):
        rows, size, files = spark.table(f"{name}.files").agg(
            F.coalesce(F.sum("record_count"), F.lit(0)), F.coalesce(F.sum("file_size_in_bytes"), F.lit(0)),
            F.count(F.lit(1))).first()
        out.append((name, rows, size, files, spark.table(f"{name}.snapshots").count()))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--record", action="store_true", help="ghi kết quả vào ops.pipeline.table_stats")
    args = ap.parse_args()

    spark = lh.get_spark("storage_report")
    try:
        stats = measure(spark)
        print(f"{'bảng':<45}{'dòng':>12}{'MiB':>10}{'file':>7}{'snapshot':>10}")
        layers = {}
        for name, rows, size, files, snapshots in stats:
            print(f"{name:<45}{rows:>12}{size / 2**20:>10.1f}{files:>7}{snapshots:>10}")
            layers[name.split(".")[0]] = layers.get(name.split(".")[0], 0) + size
        for layer, size in layers.items():
            print(f"{'  tổng ' + layer:<57}{size / 2**20:>10.1f}")
        if args.record:
            lh.ensure_ops_table(spark, lh.STATS_TABLE, lh.STATS_COLS)
            rid, now = lh.run_id(), datetime.now(timezone.utc)
            spark.createDataFrame([(rid, now, *s) for s in stats], lh.STATS_COLS) \
                 .writeTo(lh.STATS_TABLE).append()
            print(f"Đã ghi {len(stats)} dòng vào {lh.STATS_TABLE}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
