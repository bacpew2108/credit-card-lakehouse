"""Dung lượng dữ liệu đang dùng của từng tầng (mục 11 Design Doc).

    python storage_report.py

Cộng file_size_in_bytes của các file dữ liệu thuộc snapshot hiện tại (bảng metadata .files),
nên không tính snapshot cũ, file mồ côi hay metadata. Dung lượng vật lý của bucket (mc du trong
make storage-status) lớn hơn số này đúng bằng phần chờ expire_snapshots / remove_orphan_files dọn.
"""

from pyspark.sql import functions as F

from common import lakehouse as lh


def main():
    spark = lh.get_spark("storage_report")
    try:
        print(f"{'bảng':<45}{'MiB':>10}{'file':>7}{'snapshot':>10}")
        for ns in lh.NAMESPACES:
            tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {ns}").collect()]
            total = 0
            for t in tables:
                name = f"{ns}.{t}"
                size, files = spark.table(f"{name}.files").agg(
                    F.coalesce(F.sum("file_size_in_bytes"), F.lit(0)), F.count(F.lit(1))).first()
                snapshots = spark.table(f"{name}.snapshots").count()
                total += size
                print(f"{name:<45}{size / 2**20:>10.1f}{files:>7}{snapshots:>10}")
            print(f"{'  tổng ' + ns.split('.')[0]:<45}{total / 2**20:>10.1f}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
