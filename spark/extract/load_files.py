"""Nạp file tham chiếu từ bucket landing vào Bronze (mục 5.2 Design Doc).

    spark-submit load_files.py mcc [--batch-id ref_20261012] [--force]

File chỉ vài trăm dòng nên driver đọc bằng boto3 (có sẵn trong image Airflow) rồi tạo
DataFrame, không cần thêm hadoop-aws/S3A vào image.
"""

import argparse
import json
import os
import sys

import boto3
from pyspark.sql import functions as F

from common import helpers as h
from common import lakehouse as lh

MCC_BUCKET, MCC_KEY = "landing", "reference/mcc_codes/mcc_codes.jsonl"
MCC_SOURCE = f"s3://{MCC_BUCKET}/{MCC_KEY}"
MCC_TABLE = "lakehouse.bronze.mcc_codes_raw"


def read_jsonl(bucket, key):
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])  # khóa lấy từ AWS_* trong env
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def load_mcc(spark, batch_id):
    records = read_jsonl(MCC_BUCKET, MCC_KEY)
    rows = [tuple(None if r.get(c) is None else str(r[c]) for c in h.MCC_COLS) for r in records]
    df = spark.createDataFrame(rows, ", ".join(f"{c} string" for c in h.MCC_COLS))
    lh.with_metadata(df, MCC_SOURCE, batch_id).writeTo(MCC_TABLE).overwrite(F.lit(True))
    return (len(records), *lh.checksum(spark.table(MCC_TABLE), h.MCC_COLS))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dataset", choices=["mcc"])
    ap.add_argument("--batch-id", default=h.ref_batch_id())
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    if not args.batch_id.startswith("ref_"):
        ap.error("--batch-id phải bắt đầu bằng ref_")

    spark = lh.get_spark("load_files_mcc")
    try:
        lh.ensure_bronze_table(spark, MCC_TABLE, h.MCC_COLS)
        lh.run_batch(spark, lh.run_id(), MCC_SOURCE, args.batch_id,
                     lambda: load_mcc(spark, args.batch_id),
                     lh.loaded_batches(spark, MCC_SOURCE), args.force)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
