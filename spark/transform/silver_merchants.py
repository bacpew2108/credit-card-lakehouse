"""Suy ra silver.merchants từ silver.transactions (mục 5.3 Design Doc).

    spark-submit silver_merchants.py

Tính lại trên toàn bộ giao dịch rồi MERGE theo merchant_id, chỉ ghi merchant có thay đổi.
Mỗi merchant lấy vị trí (city, state, zip) có nhiều giao dịch nhất; location_count > 1 là
merchant nhiều vị trí (ghi vào DQ ở tuần 6).

silver_transactions.py gọi run() ở cuối, trong cùng Spark session: phép tính chỉ mất vài giây
khi JVM đã khởi động, còn một lần spark-submit riêng tốn 45–60 giây (khởi động, đọc lạnh).
"""

import sys
import time

from common import lakehouse as lh
from transform import silver_transforms as st

SOURCE = "lakehouse.silver.transactions"
SILVER = "lakehouse.silver.merchants"
DDL = """
    merchant_id INT, merchant_city STRING, merchant_state STRING, zip STRING,
    is_online BOOLEAN, is_foreign BOOLEAN, location_count INT, txn_count BIGINT"""


def run(spark):
    t0 = time.time()
    lh.ensure_silver_table(spark, SILVER, DDL)
    src = st.merchants(spark.table(SOURCE))
    cols = [c for c in st.MERCHANT_COLS if c != "merchant_id"]
    counts = lh.merge_changes(spark, src, SILVER, "merchant_id", cols)
    row = spark.table(SILVER).selectExpr("count(*) AS n", "count_if(location_count > 1) AS multi").first()
    print(f"[silver] {SILVER}: {row.n} merchant, {row.multi} merchant nhiều vị trí "
          f"({row.multi / max(row.n, 1):.1%}); thêm {counts['I']}, sửa {counts['U']}, "
          f"xóa {counts['D']}; {time.time() - t0:.1f}s")


def main():
    spark = lh.get_spark("silver_merchants")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
