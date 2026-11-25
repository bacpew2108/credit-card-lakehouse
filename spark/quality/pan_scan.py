"""R13: quét số thẻ đầy đủ (PAN) trong các bucket bronze, silver, gold, ops (mục 6, 7 Design Doc).

    python pan_scan.py [--bucket gold] [--workers 4]

Job đọc mọi object đang có trong bucket bằng tài khoản MinIO chỉ đọc pan-scanner, không đi qua
catalog, nên quét được cả file của snapshot cũ, file mồ côi và metadata:
- .parquet: giải mã từng cột. Cột chuỗi, nhị phân, số nguyên 64 bit, decimal, số thực đều được xét,
  kể cả phần tử nằm trong mảng, struct, map.
- Object khác (manifest .avro, metadata .json nén gzip): giải nén rồi tìm trên byte thô. Manifest giữ
  giá trị nhỏ nhất và lớn nhất của từng cột, nên số thẻ lọt vào dữ liệu cũng lộ ra ở đây.

Ứng viên là mọi chuỗi con dài 13–19 của một dãy chữ số liền nhau. Ứng viên là số thẻ thật khi
HMAC-SHA256(HMAC_SECRET, ứng viên) trùng một card_token của bronze.card.cards_raw. Cách này không
cần đến số thẻ gốc, và không báo nhầm dãy số ngẫu nhiên (checksum, đoạn chữ số trong token hex).
Không lọc bằng Luhn vì chỉ 9,7% số thẻ của bộ dữ liệu thỏa Luhn. Job cũng báo file Parquet có cột
tên card_number hoặc cvv, vì CVV chỉ 3–4 chữ số nên không nhận ra được từ giá trị.

Kết quả ghi vào ops.pipeline.dq_results (rule R13, batch scan_YYYYMMDD): checked_count là số
object đã quét, failed_count là số object vi phạm hoặc không đọc được. Có vi phạm thì BLOCK, job
thoát với mã lỗi, và job Gold không dựng tiếp cho tới khi một lần quét sau PASS. Log và details chỉ
ghi số thẻ đã che (6 số đầu, 4 số cuối). Quét riêng một bucket (--bucket) thì chỉ in, không ghi R13.
"""

import argparse
import gzip
import hmac
import io
import math
import os
import re
import sys
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from common import helpers as h

BUCKETS = ("bronze", "silver", "gold", "ops")
PAN_LENGTHS = range(13, 20)
MIN_RUN = PAN_LENGTHS[0]
DIGIT_RUN = re.compile(rb"[0-9]{%d,}" % MIN_RUN)
FORBIDDEN_COLUMNS = {"card_number", "cvv"}
MAX_SHOWN = 5                      # số vị trí vi phạm ghi vào details


# ---------------------------------------------------------------- ứng viên và đối chiếu HMAC

def digit_runs(data):
    """Các dãy chữ số liền nhau dài từ 13 trở lên (bytes) trong data (bytes hoặc str)."""
    if isinstance(data, str):
        data = data.encode("utf-8", "ignore")
    return set(DIGIT_RUN.findall(data))


def candidates(run):
    """Mọi chuỗi con dài 13–19 của một dãy chữ số: số thẻ có thể nằm lẫn trong dãy dài hơn."""
    return {run[i:i + n] for n in PAN_LENGTHS for i in range(len(run) - n + 1)}


def mask(pan):
    """b'4111111111111111' → '411111…1111': không bao giờ in số thẻ đầy đủ."""
    return f"{pan[:6].decode()}…{pan[-4:].decode()}"


class PanMatcher:
    """Nhận ra số thẻ thật trong các dãy chữ số bằng cách so HMAC với tập card_token."""

    def __init__(self, tokens, secret):
        self.digests = {bytes.fromhex(t) for t in tokens}
        self.key = secret.encode()
        self.seen = {}             # dãy chữ số → các số thẻ tìm thấy trong nó; nhiều file lặp lại cùng dãy

    def pans(self, runs):
        """Các số thẻ (bytes) nằm trong tập dãy chữ số runs."""
        found = set()
        for run in runs:
            if run not in self.seen:
                self.seen[run] = {c for c in candidates(run)
                                  if hmac.digest(self.key, c, "sha256") in self.digests}
            found |= self.seen[run]
        return found


# ---------------------------------------------------------------- đọc object

def column_runs(arr):
    """Dãy chữ số (từ 13 chữ số) trong một mảng pyarrow. Kiểu không thể chứa 13 chữ số (số nguyên
    dưới 64 bit, boolean, ngày giờ) được bỏ qua."""
    import pyarrow as pa
    import pyarrow.compute as pc

    t = arr.type
    if pa.types.is_dictionary(t):
        return column_runs(arr.dictionary)
    if pa.types.is_decimal(t):
        return column_runs(arr.cast(pa.string())) if t.precision >= MIN_RUN else set()
    if (pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_binary(t)
            or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t)):
        if len(arr) == arr.null_count or pc.max(pc.binary_length(arr)).as_py() < MIN_RUN:
            return set()
        hit = arr.filter(pc.match_substring_regex(arr, "[0-9]{%d}" % MIN_RUN))
        return set().union(*(digit_runs(v) for v in hit.to_pylist()))
    if pa.types.is_integer(t):
        if t.bit_width < 64:
            return set()
        hit = pc.greater_equal(arr, 10 ** (MIN_RUN - 1))
        if pa.types.is_signed_integer(t):
            hit = pc.or_(hit, pc.less_equal(arr, -10 ** (MIN_RUN - 1)))
        return {str(abs(v)).encode() for v in arr.filter(hit).to_pylist()}
    if pa.types.is_floating(t):
        hit = pc.greater_equal(pc.abs(arr), float(10 ** (MIN_RUN - 1)))
        return {f"{abs(v):.0f}".encode() for v in arr.filter(hit).to_pylist() if math.isfinite(v)}
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        return column_runs(arr.flatten())
    if pa.types.is_map(t):
        return column_runs(arr.keys) | column_runs(arr.items)
    if pa.types.is_struct(t):
        return set().union(*(column_runs(arr.field(i)) for i in range(t.num_fields)))
    return set()


def scan_parquet(data):
    """File Parquet (bytes) → (dãy chữ số trong mọi cột, cột cấm có trong schema)."""
    import pyarrow.parquet as pq

    f = pq.ParquetFile(io.BytesIO(data))
    names = {part.lower() for i in range(len(f.schema)) for part in f.schema.column(i).path.split(".")}
    runs = set()
    for batch in f.iter_batches(batch_size=1 << 16):
        for col in batch.columns:
            runs |= column_runs(col)
    return runs, sorted(names & FORBIDDEN_COLUMNS)


def _avro_long(buf, pos):
    """Số long của Avro (varint zigzag) tại pos → (giá trị, vị trí kế tiếp)."""
    n = shift = 0
    while True:
        b = buf[pos]
        pos += 1
        n |= (b & 0x7F) << shift
        if not b & 0x80:
            return (n >> 1) ^ -(n & 1), pos
        shift += 7


def avro_payload(data):
    """File Avro object container → header (schema, thuộc tính) nối với các block đã giải nén.
    Không cần tách từng bản ghi: chuỗi trong Avro là byte UTF-8 liền nhau."""
    pos, meta = 4, {}
    while True:
        count, pos = _avro_long(data, pos)
        if count == 0:
            break
        if count < 0:                       # block có kèm kích thước tính bằng byte
            count = -count
            _, pos = _avro_long(data, pos)
        for _ in range(count):
            n, pos = _avro_long(data, pos)
            key = data[pos:pos + n]
            n, pos = _avro_long(data, pos + n)
            meta[key] = data[pos:pos + n]
            pos += n
    codec = meta.get(b"avro.codec", b"null")
    if codec not in (b"null", b"deflate"):
        raise ValueError(f"codec Avro {codec.decode()} chưa được hỗ trợ")
    parts = [data[:pos]]
    pos += 16                               # sync marker
    while pos < len(data):
        _, pos = _avro_long(data, pos)      # số bản ghi của block
        size, pos = _avro_long(data, pos)
        block = data[pos:pos + size]
        parts.append(zlib.decompress(block, -15) if codec == b"deflate" else block)
        pos += size + 16
    return b"\n".join(parts)


def decode(data):
    """Nội dung đã giải nén của một object không phải Parquet: gzip (metadata.json do Lakekeeper
    ghi), Avro (manifest), còn lại giữ nguyên."""
    if data[:2] == b"\x1f\x8b":
        return gzip.decompress(data)
    if data[:4] == b"Obj\x01":
        return avro_payload(data)
    return data


def scan_object(key, data):
    """Một object → (dãy chữ số tìm thấy, cột cấm)."""
    if key.endswith(".parquet"):
        return scan_parquet(data)
    return digit_runs(decode(data)), []


# ---------------------------------------------------------------- quét bucket

def s3_client():
    import boto3

    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                        aws_access_key_id=os.environ["PAN_SCAN_ACCESS_KEY"],
                        aws_secret_access_key=os.environ["PAN_SCAN_SECRET_KEY"])


def scan_bucket(s3, bucket, matcher, workers=4):
    """Quét mọi object của bucket → {objects, bytes, runs, findings, unreadable}.
    findings: [(key, [số thẻ đã che], [cột cấm])]; unreadable: [(key, lỗi)]."""
    keys = [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket)
            for o in page.get("Contents", [])]

    def scan(key):
        try:
            data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except s3.exceptions.NoSuchKey:     # bị job bảo trì xóa sau khi liệt kê
            return None
        try:
            runs, banned = scan_object(key, data)
        except Exception as e:
            return key, len(data), 0, None, f"{type(e).__name__}: {e}"[:200]
        pans = sorted(mask(p) for p in matcher.pans(runs))
        return key, len(data), len(runs), (pans, banned), None

    out = {"objects": 0, "bytes": 0, "runs": 0, "findings": [], "unreadable": []}
    with ThreadPoolExecutor(workers) as pool:
        for res in pool.map(scan, keys):
            if res is None:
                continue
            key, size, n_runs, hit, error = res
            out["objects"] += 1
            out["bytes"] += size
            out["runs"] += n_runs
            if error:
                out["unreadable"].append((key, error))
            elif hit[0] or hit[1]:
                out["findings"].append((key, *hit))
    return out


def summarize(results):
    """{bucket: kết quả scan_bucket} → (checked, failed, details) của R13."""
    checked = sum(r["objects"] for r in results.values())
    found = [(b, *f) for b, r in results.items() for f in r["findings"]]
    unreadable = [(b, *u) for b, r in results.items() for u in r["unreadable"]]
    n_pan = sum(bool(pans) for _, _, pans, _ in found)
    n_col = sum(bool(banned) for _, _, _, banned in found)
    details = (f"{len(results)} bucket, {checked} object, {sum(r['bytes'] for r in results.values()) / 2**20:.0f} MiB, "
               f"{sum(r['runs'] for r in results.values())} dãy từ {MIN_RUN} chữ số; object có số thẻ: {n_pan}; "
               f"có cột card_number/cvv: {n_col}; không đọc được: {len(unreadable)}")
    shown = [f"s3://{b}/{key} ({', '.join(pans + banned)})" for b, key, pans, banned in found[:MAX_SHOWN]]
    shown += [f"s3://{b}/{key} ({error})" for b, key, error in unreadable[:MAX_SHOWN]]
    if shown:
        details += " – " + "; ".join(shown)
    return checked, len(found) + len(unreadable), details


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", action="append", choices=BUCKETS,
                    help="chỉ quét bucket này (lặp lại được); khi đó không ghi kết quả R13")
    ap.add_argument("--workers", type=int, default=4, help="số luồng đọc object (mặc định %(default)s)")
    args = ap.parse_args()
    buckets = tuple(b for b in BUCKETS if b in args.bucket) if args.bucket else BUCKETS
    secret = h.require_secret(os.environ.get("HMAC_SECRET"))

    from common import lakehouse as lh
    from quality import rules as dq

    spark = lh.get_spark("pan_scan")
    try:
        tokens = {r.card_token for r in spark.table(h.BRONZE_CARDS).select("card_token").collect() if r.card_token}
        if not tokens:
            raise RuntimeError(f"{h.BRONZE_CARDS} chưa có card_token để đối chiếu (make bronze-ref)")
        matcher, s3, results = PanMatcher(tokens, secret), s3_client(), {}
        for bucket in buckets:
            t0 = time.time()
            r = results[bucket] = scan_bucket(s3, bucket, matcher, args.workers)
            print(f"[R13] {bucket}: {r['objects']} object, {r['bytes'] / 2**20:.0f} MiB, {r['runs']} dãy chữ số, "
                  f"{len(r['findings'])} vi phạm, {len(r['unreadable'])} không đọc được; {time.time() - t0:.0f}s")
        checked, failed, details = summarize(results)
        if buckets == BUCKETS:
            batch_id = f"scan_{datetime.now(timezone.utc):%Y%m%d}"
            dq.record(spark, lh.run_id(), batch_id, [("R13", checked, failed, details)])
        else:
            print(f"[R13] chưa quét đủ {len(BUCKETS)} bucket, không ghi dq_results: {details}")
    finally:
        spark.stop()
    if failed:
        sys.exit(f"[BLOCK] R13: {details}")


if __name__ == "__main__":
    sys.exit(main())
