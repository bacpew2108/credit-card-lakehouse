"""Suy ra silver.card.merchants từ silver.card.transactions (mục 5.3 Design Doc).

    python silver_merchants.py

Tính lại trên toàn bộ giao dịch rồi MERGE theo merchant_id, chỉ ghi merchant có thay đổi.
Mỗi merchant lấy vị trí (city, state, zip) có nhiều giao dịch nhất; location_count > 1 là
merchant nhiều vị trí, ghi vào ops.pipeline.dq_results với rule I01 (chỉ ghi nhận, không chặn).

silver_transactions.py gọi run() ở cuối, trong cùng Spark session, để mỗi lần dựng Silver
giao dịch thì merchants cũng được cập nhật (bỏ qua bằng --skip-merchants).
"""

import sys
import time

from common import helpers as h
from common import lakehouse as lh
from quality import rules as dq
from transform import silver_transforms as st

SOURCE = h.SILVER_TXN
SILVER = h.SILVER_MERCHANTS
DDL = """
    merchant_id INT, merchant_city STRING, merchant_state STRING, zip STRING,
    is_online BOOLEAN, is_foreign BOOLEAN, location_count INT, txn_count BIGINT"""


def run(spark, rid=None, batch_id=None):
    """batch_id của kết quả I01: tháng vừa dựng Silver; None thì lấy tháng mới nhất trong bảng."""
    t0 = time.time()
    lh.ensure_silver_table(spark, SILVER, DDL)
    src = st.merchants(spark.table(SOURCE))
    cols = [c for c in st.MERCHANT_COLS if c != "merchant_id"]
    counts = lh.merge_changes(spark, src, SILVER, "merchant_id", cols)
    row = spark.table(SILVER).selectExpr("count(*) AS n", "count_if(location_count > 1) AS multi",
                                         "max(_batch_id) AS batch").first()
    print(f"[silver] {SILVER}: {row.n} merchant, {row.multi} merchant nhiều vị trí "
          f"({row.multi / max(row.n, 1):.1%}); thêm {counts['I']}, sửa {counts['U']}, "
          f"xóa {counts['D']}; {time.time() - t0:.1f}s")
    dq.record(spark, rid or lh.run_id(), batch_id or row.batch, [("I01", row.n, row.multi, None)])


def main():
    spark = lh.get_spark("silver_merchants")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
