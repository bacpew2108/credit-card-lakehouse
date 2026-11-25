"""Kiểm thử R13 (spark/quality/pan_scan.py): số thẻ cố ý cài vào file phải bị phát hiện ở mọi kiểu
cột và mọi loại object; dãy số không phải số thẻ không bị báo nhầm; số thẻ không bao giờ được in ra."""

import gzip
import io
import json
import zlib
from decimal import Decimal

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

from common import helpers as h  # noqa: E402
from quality import pan_scan as ps  # noqa: E402

SECRET = "test-secret"
PANS = ["4111111111111111", "378282246310005", "4222222222222", "6011000990139424123"]   # 16, 15, 13, 19 số
PAN = PANS[0]


@pytest.fixture
def matcher():
    return ps.PanMatcher([h.protect_pan(p, SECRET)[0] for p in PANS], SECRET)


def found(matcher, key, data):
    runs, _ = ps.scan_object(key, data)
    return {p.decode() for p in matcher.pans(runs)}


def parquet(columns):
    buf = io.BytesIO()
    pq.write_table(pa.table(columns), buf)
    return buf.getvalue()


def avro_long(n):
    n, out = (n << 1) ^ (n >> 63), b""
    while n > 0x7F:
        out += bytes([n & 0x7F | 0x80])
        n >>= 7
    return out + bytes([n])


def avro_file(payload, codec):
    """File Avro object container tối thiểu: header rồi một block chứa payload."""
    meta = {b"avro.schema": b'{"type": "bytes"}', b"avro.codec": codec}
    header = b"Obj\x01" + avro_long(len(meta))
    for k, v in meta.items():
        header += avro_long(len(k)) + k + avro_long(len(v)) + v
    sync = bytes(range(16))
    if codec == b"deflate":
        c = zlib.compressobj(wbits=-15)
        payload = c.compress(payload) + c.flush()
    return header + avro_long(0) + sync + avro_long(1) + avro_long(len(payload)) + payload + sync


def test_candidates_are_all_substrings_of_13_to_19_digits():
    assert ps.candidates(b"1" * 12) == set()
    run = b"1234567890123456"
    got = ps.candidates(run)
    assert len(got) == 4 + 3 + 2 + 1 and run in got and b"2345678901234" in got
    assert max(map(len, ps.candidates(b"9" * 30))) == 19


@pytest.mark.parametrize("pan", PANS)
def test_pan_is_recognised_by_hmac_even_inside_a_longer_digit_run(matcher, pan):
    assert matcher.pans(ps.digit_runs(f"id=7;card {pan};")) == {pan.encode()}
    assert matcher.pans(ps.digit_runs(f"00{pan}9")) == {pan.encode()}


def test_digit_runs_that_are_not_cards_are_ignored(matcher):
    token = h.protect_pan(PAN, SECRET)[0]
    text = f"{token} 4111111111111112 99887766554433221100 checksum -1234567890123456789012"
    assert len(ps.digit_runs(text)) >= 3
    assert matcher.pans(ps.digit_runs(text)) == set()
    assert ps.digit_runs("2019-03-01 00:01:00 $-77.00 4111 1111 1111") == set()


@pytest.mark.parametrize("name, column", [
    ("string", pa.array(["ok", f"số thẻ {PAN}", None])),
    ("binary", pa.array([b"x", PAN.encode()], pa.binary())),
    ("bigint", pa.array([7, int(PAN), -5], pa.int64())),
    ("negative bigint", pa.array([-int(PAN)], pa.int64())),
    ("double", pa.array([1.5, float(PAN)], pa.float64())),
    ("decimal", pa.array([None, Decimal(PAN)], pa.decimal128(20, 0))),
    ("list", pa.array([["a"], ["b", PAN]], pa.list_(pa.string()))),
    ("struct", pa.array([{"note": PAN, "n": 1}], pa.struct([("note", pa.string()), ("n", pa.int32())]))),
    ("map", pa.array([[("card", PAN)]], pa.map_(pa.string(), pa.string()))),
    ("dictionary", pa.array(["a", PAN, "a"]).dictionary_encode()),
])
def test_parquet_leak_is_found_in_every_column_type(matcher, name, column):
    data = parquet({"id": pa.array(range(len(column)), pa.int32()), "leak": column})
    assert found(matcher, "t/data/00000-0.parquet", data) == {PAN}, name


def test_clean_parquet_like_real_tables_has_no_finding(matcher):
    token, card_bin, last4 = h.protect_pan(PAN, SECRET)
    data = parquet({
        "transaction_id": pa.array([7475327, 23761873], pa.int64()),
        "date": pa.array(["2019-03-01 00:01:00", "2019-03-31 23:59:00"]),
        "amount": pa.array(["$-77.00", "$29278"]),
        "card_token": pa.array([token, token]),
        "card_bin": pa.array([card_bin, card_bin]),
        "card_last4": pa.array([last4, last4]),
        "checksum": pa.array(["-123456789012345678901", "98765432109876543210"]),
        "error_list": pa.array([[], ["Bad PIN"]], pa.list_(pa.string())),
    })
    runs, banned = ps.scan_object("t/data/00000-0.parquet", data)
    assert runs and matcher.pans(runs) == set() and banned == []


def test_forbidden_columns_are_reported():
    data = parquet({"id": pa.array([1]), "CVV": pa.array(["123"]), "Card_Number": pa.array(["x"])})
    assert ps.scan_object("t/data/a.parquet", data)[1] == ["card_number", "cvv"]


def test_metadata_and_manifest_files_are_decompressed(matcher):
    meta = gzip.compress(json.dumps({"summary": {"note": PAN}}).encode())
    assert found(matcher, "t/metadata/00001-x.gz.metadata.json", meta) == {PAN}
    bounds = b"\x02\x20" + PAN.encode() + b"\x00"          # lower_bound của một cột chuỗi trong manifest
    for codec in (b"deflate", b"null"):
        assert found(matcher, "t/metadata/x-m0.avro", avro_file(bounds, codec)) == {PAN}, codec
    assert found(matcher, "t/metadata/x-m0.avro", avro_file(b"\x02\x08abcd", b"deflate")) == set()
    with pytest.raises(ValueError, match="snappy"):
        ps.scan_object("t/metadata/x-m0.avro", avro_file(bounds, b"snappy"))


class FakeS3:
    """Đủ phần của boto3 client mà scan_bucket dùng."""

    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self, objects, listed):
        self.objects, self.listed = objects, listed

    def get_paginator(self, _):
        return self

    def paginate(self, Bucket):
        return [{"Contents": [{"Key": k} for k in self.listed[:2]]}, {"Contents": [{"Key": k} for k in self.listed[2:]]}, {}]

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}


def test_scan_bucket_and_summary_never_print_the_full_pan(matcher):
    objects = {
        "a/data/clean.parquet": parquet({"amount": pa.array(["$1.00"])}),
        "a/data/leak.parquet": parquet({"note": pa.array([PAN, PANS[1]])}),
        "a/data/cvv.parquet": parquet({"cvv": pa.array(["123"])}),
        "a/data/broken.parquet": b"not a parquet file",
        "a/metadata/v1.gz.metadata.json": gzip.compress(b'{"format-version": 2}'),
    }
    s3 = FakeS3(objects, [*objects, "a/data/deleted-meanwhile.parquet"])
    r = ps.scan_bucket(s3, "gold", matcher, workers=2)
    assert r["objects"] == 5 and r["bytes"] == sum(map(len, objects.values()))
    assert r["findings"] == [("a/data/leak.parquet", ["378282…0005", "411111…1111"], []),
                             ("a/data/cvv.parquet", [], ["cvv"])]
    assert [k for k, _ in r["unreadable"]] == ["a/data/broken.parquet"]

    checked, failed, details = ps.summarize({"gold": r, "ops": {**r, "findings": [], "unreadable": []}})
    assert (checked, failed) == (10, 3)
    assert "object có số thẻ: 1; có cột card_number/cvv: 1; không đọc được: 1" in details
    assert "s3://gold/a/data/leak.parquet (378282…0005, 411111…1111)" in details
    assert not any(p in details or p in str(r) for p in PANS)

    clean = ps.scan_bucket(FakeS3(objects, ["a/data/clean.parquet", "a/metadata/v1.gz.metadata.json"]), "ops", matcher)
    assert ps.summarize({"ops": clean})[:2] == (2, 0)
