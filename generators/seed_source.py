"""Nạp dữ liệu vào hệ thống nguồn giả lập core_card (tuần 3, mục 3.3 Design Doc).

Nạp toàn bộ customers, cards, transactions (13,3 triệu dòng) vào PostgreSQL trong một lần
và đưa mcc_codes (JSON Lines) lên bucket landing của MinIO. Pipeline phía sau vẫn trích xuất
giao dịch theo từng tháng (lọc theo date).

Chạy trên máy host, kết nối postgres-source qua cổng SOURCE_DB_HOST_PORT và MinIO qua
localhost:9000. Thông tin kết nối lấy từ biến môi trường, thiếu thì đọc Code/.env.

    python generators/seed_source.py load [--force]   # bỏ qua nếu đã có dữ liệu, trừ khi --force
    python generators/seed_source.py status
"""

import argparse
import csv
import io
import json
import os
import time
from pathlib import Path

import psycopg

CODE_DIR = Path(__file__).resolve().parents[1]
INIT_DIR = CODE_DIR / "infra" / "postgres-source" / "init"
SCHEMA_SQL = INIT_DIR / "01_core_card.sql"
CONSTRAINTS_SQL = INIT_DIR / "02_core_card_constraints.sql"
DEFAULT_DATA_DIR = CODE_DIR / "data" / "raw"

LANDING_BUCKET = "landing"
MCC_OBJECT = "reference/mcc_codes/mcc_codes.jsonl"

CUSTOMER_COLS = [
    "id", "current_age", "retirement_age", "birth_year", "birth_month", "gender", "address",
    "latitude", "longitude", "per_capita_income", "yearly_income", "total_debt",
    "credit_score", "num_credit_cards",
]
# card_on_dark_web của file gốc bị bỏ: chỉ có một giá trị "No" và không có trong Hình 1
CARD_COLS = [
    "id", "client_id", "card_brand", "card_type", "card_number", "expires", "cvv", "has_chip",
    "num_cards_issued", "credit_limit", "acct_open_date", "year_pin_last_changed",
]
# Trùng thứ tự cột của transactions_data.csv nên file được đẩy thẳng vào COPY
TXN_COLS = [
    "id", "date", "client_id", "card_id", "amount", "use_chip", "merchant_id",
    "merchant_city", "merchant_state", "zip", "mcc", "errors",
]


# ---------------------------------------------------------------- Hàm thuần (có pytest)

def mcc_to_jsonl(mapping: dict[str, str]) -> str:
    """mcc_codes.json là một JSON object duy nhất → JSON Lines, mỗi dòng một mã."""
    lines = [
        json.dumps({"mcc": code, "description": desc}, ensure_ascii=False)
        for code, desc in sorted(mapping.items())
    ]
    return "\n".join(lines) + "\n"


def read_csv_rows(path: Path, cols: list[str]) -> list[tuple]:
    """Đọc các cột cần lấy, giữ dạng chuỗi như hệ thống cũ; ô rỗng thành NULL."""
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        missing = set(cols) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name} thiếu cột {sorted(missing)}")
        return [tuple(r[c] or None for c in cols) for r in reader]


def csv_header(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return next(csv.reader(f))


def load_env_file(path: Path) -> None:
    """Đọc KEY=VALUE từ .env; biến đã có trong môi trường được ưu tiên."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


# ---------------------------------------------------------------- PostgreSQL, MinIO

def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("SOURCE_DB_HOST", "localhost"),
        port=int(os.environ.get("SOURCE_DB_PORT") or os.environ.get("SOURCE_DB_HOST_PORT", "5434")),
        user=os.environ["SOURCE_DB_USER"],
        password=os.environ["SOURCE_DB_PASSWORD"],
        dbname=os.environ["SOURCE_DB_NAME"],
        autocommit=True,  # mỗi khối conn.transaction() là một transaction thật
    )


def copy_rows(cur, table: str, cols: list[str], rows) -> None:
    with cur.copy(f"COPY {table} ({', '.join(cols)}) FROM STDIN") as cp:
        for r in rows:
            cp.write_row(r)


def copy_transactions(cur, path: Path, chunk: int = 8 << 20) -> int:
    """Đẩy nguyên file CSV vào COPY: Postgres tự parse, Python không phải đọc từng dòng.

    HEADER MATCH bắt Postgres kiểm tra dòng tiêu đề trùng danh sách cột. Trường errors có
    nhiều lỗi được bọc "…" theo chuẩn CSV; ô rỗng không bọc nháy thành NULL.
    """
    size, sent, step = path.stat().st_size, 0, 0
    sql = f"COPY transactions ({', '.join(TXN_COLS)}) FROM STDIN (FORMAT csv, HEADER MATCH)"
    with path.open("rb") as f, cur.copy(sql) as cp:
        while data := f.read(chunk):
            cp.write(data)
            sent += len(data)
            if sent * 10 // size > step:
                step = sent * 10 // size
                print(f"  transactions: {step * 10}% ({sent >> 20:,} MB)", flush=True)
    return cur.rowcount


def upload_mcc(jsonl: str) -> str:
    from minio import Minio

    client = Minio(
        os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
        region="us-east-1",
    )
    data = jsonl.encode()
    client.put_object(LANDING_BUCKET, MCC_OBJECT, io.BytesIO(data), len(data),
                      content_type="application/x-ndjson")
    return f"s3://{LANDING_BUCKET}/{MCC_OBJECT}"


def load_all(conn, data_dir: Path, force: bool) -> None:
    n_txn = conn.execute("SELECT count(*) FROM transactions").fetchone()[0]
    if n_txn and not force:
        print(f"core_card đã có {n_txn:,} giao dịch → bỏ qua (thêm FORCE=1 để nạp lại)")
        return

    txn_file = data_dir / "transactions_data.csv"
    if csv_header(txn_file) != TXN_COLS:
        raise ValueError(f"Tiêu đề {txn_file.name} khác thứ tự cột của bảng transactions")
    customers = read_csv_rows(data_dir / "users_data.csv", CUSTOMER_COLS)
    cards = read_csv_rows(data_dir / "cards_data.csv", CARD_COLS)

    t0 = time.time()
    # Một transaction: lỗi giữa chừng thì dữ liệu cũ còn nguyên
    with conn.transaction(), conn.cursor() as cur:
        # Bỏ PK, FK, index của transactions trong lúc nạp rồi tạo lại một lần ở cuối
        cur.execute("ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_pkey, "
                    "DROP CONSTRAINT IF EXISTS transactions_client_id_fkey, "
                    "DROP CONSTRAINT IF EXISTS transactions_card_id_fkey")
        cur.execute("DROP INDEX IF EXISTS transactions_date_idx")
        cur.execute("TRUNCATE transactions, cards, customers")
        copy_rows(cur, "customers", CUSTOMER_COLS, customers)
        copy_rows(cur, "cards", CARD_COLS, cards)
        print(f"customers: {len(customers):,} dòng, cards: {len(cards):,} dòng")
        n = copy_transactions(cur, txn_file)
        print(f"transactions: {n:,} dòng sau {time.time() - t0:.0f} giây; tạo PK, FK, index…")
        cur.execute(CONSTRAINTS_SQL.read_text())
    conn.execute("ANALYZE transactions")
    print(f"Nạp xong core_card sau {time.time() - t0:.0f} giây")

    mcc = json.loads((data_dir / "mcc_codes.json").read_text())
    print(f"mcc_codes: {len(mcc)} mã → {upload_mcc(mcc_to_jsonl(mcc))}")


def print_status(conn) -> None:
    for table in ("customers", "cards", "transactions"):
        n = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"{table:<13} {n:>12,} dòng")
    first, last, months = conn.execute(
        "SELECT min(date), max(date), count(DISTINCT date_trunc('month', date)) FROM transactions"
    ).fetchone()
    if first:
        print(f"Giao dịch từ {first:%Y-%m-%d} đến {last:%Y-%m-%d}, {months} tháng")


# ---------------------------------------------------------------- CLI

def main():
    load_env_file(CODE_DIR / ".env")
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data-dir", type=Path,
                    default=Path(os.environ.get("DATA_DIR") or DEFAULT_DATA_DIR))
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("load", help="Nạp toàn bộ dữ liệu nguồn")
    p.add_argument("--force", action="store_true", help="Xóa và nạp lại dù đã có dữ liệu")
    sub.add_parser("status", help="Số dòng từng bảng, khoảng thời gian giao dịch")
    args = ap.parse_args()

    with connect() as conn:
        conn.execute(SCHEMA_SQL.read_text())
        if args.cmd == "load":
            load_all(conn, args.data_dir, args.force)
        else:
            print_status(conn)


if __name__ == "__main__":
    main()
