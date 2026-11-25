"""Chạy truy vấn phân tích Q1–Q10 trên Trino và đối chiếu với tính toán độc lập bằng DuckDB.

    python sql/run_queries.py run                  # chạy mọi truy vấn trong sql/queries, in kết quả
    python sql/run_queries.py run q03 q05b         # chỉ các truy vấn có tên bắt đầu bằng q03, q05b
    python sql/run_queries.py verify               # so từng truy vấn với sql/independent trên CSV gốc
    python sql/run_queries.py roles                # kiểm tra phân quyền 2 vai trò engineer, analyst

Truy vấn Trino (sql/queries/*.sql) chạy bằng user của vai trò analyst (mặc định `analyst`, cùng nhóm
với tài khoản Superset), nên chỉ đọc được gold và ops như dashboard.

Tính toán độc lập (sql/independent/*.sql) đọc thẳng file gốc trong data/raw bằng DuckDB, tự làm sạch
theo định nghĩa trong docs/data_model.md, không đi qua Bronze, Silver, Gold. Hai bên phải ra cùng tên
cột; các dòng được ghép theo cột khóa khai báo ở dòng `-- khóa:` của truy vấn Trino. Số nguyên và số
thập phân (decimal) phải bằng nhau tuyệt đối; số thực đã làm tròn được lệch tối đa một đơn vị ở chữ
số cuối của cột (phép làm tròn ở hai engine có thể khác nhau tại đúng điểm giữa).
"""

import argparse
import json
import sys
import time
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent
QUERIES = ROOT / "queries"
INDEPENDENT = ROOT / "independent"
REPO = ROOT.parent
MCC_GROUPS = REPO / "spark" / "transform" / "ref" / "mcc_groups.csv"


# ---------------------------------------------------------------- truy vấn

def query_files(prefixes):
    files = sorted(QUERIES.glob("q*.sql"))
    if prefixes:
        files = [f for f in files if any(f.stem.startswith(p) for p in prefixes)]
        if not files:
            raise SystemExit(f"Không có truy vấn nào bắt đầu bằng {', '.join(prefixes)} trong {QUERIES}")
    return files


def keys_of(sql_text):
    for line in sql_text.splitlines():
        if line.startswith("-- khóa:"):
            return [k.strip() for k in line.split(":", 1)[1].split(",")]
    raise ValueError("thiếu dòng '-- khóa: …'")


def trino_conn(args):
    import trino
    return trino.dbapi.connect(host=args.host, port=args.port, user=args.user, catalog="gold", schema="card",
                               http_scheme="http")


def run_trino(conn, sql_text):
    cur = conn.cursor()
    t0 = time.time()
    cur.execute(sql_text)
    rows = cur.fetchall()
    return [d[0] for d in cur.description], [tuple(r) for r in rows], time.time() - t0


# ---------------------------------------------------------------- DuckDB trên CSV gốc

def erased_customers(args):
    """customer_id trong danh sách xóa ops.privacy.erasure_log (make erase). Chỉ vai trò engineer đọc
    được bảng này; bảng chưa có (chưa xóa ai) thì rỗng."""
    import trino
    conn = trino.dbapi.connect(host=args.host, port=args.port, user="engineer", http_scheme="http")
    try:
        _, rows, _ = run_trino(conn, "SELECT DISTINCT customer_id FROM ops.privacy.erasure_log")
    except trino.exceptions.TrinoUserError as e:
        if e.error_name in ("TABLE_NOT_FOUND", "SCHEMA_NOT_FOUND"):
            return []
        raise
    return sorted(r[0] for r in rows)


def duck_conn(data_dir, erased=()):
    """DuckDB in-memory với các bảng tx, customers, cards, mcc, dq_month dựng từ file gốc (_base.sql).
    erased: khách hàng đã xóa theo yêu cầu, _base.sql ẩn danh giao dịch của họ như erase_customer.py."""
    import duckdb
    data_dir = Path(data_dir).resolve()
    con = duckdb.connect()
    con.execute("CREATE TABLE erased (customer_id INTEGER)")
    if erased:
        con.executemany("INSERT INTO erased VALUES (?)", [(int(c),) for c in erased])
    con.execute("CREATE TABLE mcc (mcc_code INTEGER, description VARCHAR)")
    codes = json.loads((data_dir / "mcc_codes.json").read_text())
    con.executemany("INSERT INTO mcc VALUES (?, ?)", [(int(k), v) for k, v in codes.items()])
    base = (INDEPENDENT / "_base.sql").read_text()
    t0 = time.time()
    con.execute(base.replace("{data_dir}", str(data_dir)).replace("{mcc_groups}", str(MCC_GROUPS)))
    n = con.execute("SELECT count(*) FROM tx").fetchone()[0]
    print(f"[duckdb] đọc {n:,} giao dịch từ {data_dir}, {time.time() - t0:.0f}s"
          + (f"; ẩn danh giao dịch của {len(erased)} khách hàng đã xóa {list(erased)}" if erased else ""))
    return con


def run_duck(con, sql_text):
    t0 = time.time()
    cur = con.execute(sql_text)
    rows = cur.fetchall()
    return [d[0] for d in cur.description], [tuple(r) for r in rows], time.time() - t0


# ---------------------------------------------------------------- so sánh

def decimals(v):
    s = repr(float(v))
    return 0 if "e" in s or "." not in s else len(s.split(".")[1].rstrip("0"))


def same(a, b, tol):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        return abs(float(a) - float(b)) <= tol
    if isinstance(a, (int, Decimal)) and isinstance(b, (int, Decimal)):
        return Decimal(a) == Decimal(b)
    return a == b


def compare(keys, t_cols, t_rows, d_cols, d_rows):
    """→ danh sách mô tả chỗ lệch (rỗng nếu khớp)."""
    if t_cols != d_cols:
        return [f"cột khác nhau: trino {t_cols} / duckdb {d_cols}"]
    missing = [k for k in keys if k not in t_cols]
    if missing:
        return [f"cột khóa {missing} không có trong kết quả"]
    ki = [t_cols.index(k) for k in keys]
    # Dung sai của cột số thực: một đơn vị ở chữ số cuối, theo số chữ số thập phân lớn nhất của cột
    tol = {}
    for i in range(len(t_cols)):
        vals = [r[i] for r in t_rows + d_rows if isinstance(r[i], float)]
        tol[i] = 10 ** -max(decimals(v) for v in vals) * 1.000001 if vals else 0
    left = {tuple(r[i] for i in ki): r for r in t_rows}
    right = {tuple(r[i] for i in ki): r for r in d_rows}
    issues = []
    for k in sorted(set(left) - set(right), key=str):
        issues.append(f"chỉ có ở trino: {dict(zip(keys, k))}")
    for k in sorted(set(right) - set(left), key=str):
        issues.append(f"chỉ có ở duckdb: {dict(zip(keys, k))}")
    for k in sorted(set(left) & set(right), key=str):
        for i, c in enumerate(t_cols):
            a, b = left[k][i], right[k][i]
            if not same(a, b, tol[i]):
                issues.append(f"{dict(zip(keys, k))} {c}: trino {a} / duckdb {b}")
    if len(t_rows) != len(left) or len(d_rows) != len(right):
        issues.append("khóa không duy nhất trong kết quả")
    return issues


# ---------------------------------------------------------------- in kết quả

def fmt(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:,.4f}".rstrip("0").rstrip(".")
    if isinstance(v, (int, Decimal)) and not isinstance(v, bool):
        # Không tách hàng nghìn với số nhỏ: năm, mã MCC, giờ
        return f"{v:,}" if abs(v) >= 10000 else str(v)
    return str(v)


def print_table(cols, rows, limit=40):
    cells = [[fmt(v) for v in r] for r in rows[:limit]]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(cols)]
    print("  " + " | ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("  " + "-+-".join("-" * w for w in widths))
    for r in cells:
        print("  " + " | ".join(v.rjust(w) if v[:1].isdigit() or v[:1] == "-" else v.ljust(w)
                               for v, w in zip(r, widths)))
    if len(rows) > limit:
        print(f"  … {len(rows) - limit} dòng nữa")


# ---------------------------------------------------------------- phân quyền

# (user, mô tả, câu lệnh, được phép?). engineer, analyst là user thuộc nhóm cùng tên trong
# infra/trino/groups.txt; guest không thuộc nhóm nào. Bảng tạm role_check tạo rồi xóa ngay.
ROLE_CHECKS = [
    ("engineer", "đọc bronze", "SELECT count(*) FROM bronze.card.cards_raw", True),
    ("engineer", "đọc silver", "SELECT count(*) FROM silver.card.customers", True),
    ("engineer", "đọc gold", "SELECT count(*) FROM gold.card.dim_mcc", True),
    ("engineer", "tạo bảng trong ops", "CREATE TABLE ops.pipeline.role_check AS SELECT 1 AS x", True),
    ("engineer", "xóa bảng trong ops", "DROP TABLE ops.pipeline.role_check", True),
    ("engineer", "ghi gold (catalog chỉ đọc)", "DELETE FROM gold.card.dim_mcc WHERE mcc_key = -100", False),
    ("analyst", "đọc gold", "SELECT count(*) FROM gold.card.mart_monthly_kpi", True),
    ("analyst", "đọc ops", "SELECT count(*) FROM ops.pipeline.dq_results", True),
    ("analyst", "đọc silver", "SELECT count(*) FROM silver.card.customers", False),
    ("analyst", "đọc bronze", "SELECT count(*) FROM bronze.card.cards_raw", False),
    ("analyst", "đọc danh sách xóa", "SELECT count(*) FROM ops.privacy.erasure_log", False),
    ("analyst", "tạo bảng trong ops", "CREATE TABLE ops.pipeline.role_check AS SELECT 1 AS x", False),
    ("analyst", "ghi vào ops", "DELETE FROM ops.pipeline.smoke_test WHERE id = -100", False),
    ("guest", "đọc gold", "SELECT count(*) FROM gold.card.dim_mcc", False),
]


def cmd_roles(args):
    import trino
    unexpected = 0
    print(f"  {'user':<9} {'thao tác':<28} {'kết quả':<10} {'mong đợi':<10} lý do")
    for user, what, sql_text, allowed in ROLE_CHECKS:
        conn = trino.dbapi.connect(host=args.host, port=args.port, user=user, http_scheme="http")
        try:
            cur = conn.cursor()
            cur.execute(sql_text)
            cur.fetchall()
            ok, reason = True, ""
        except trino.exceptions.TrinoUserError as e:
            ok, reason = False, e.message.split("\n")[0][:70]
        hit = ok == allowed
        unexpected += not hit
        print(f"  {user:<9} {what:<28} {'được' if ok else 'bị chặn':<10} {'được' if allowed else 'bị chặn':<10} "
              f"{reason}{'' if hit else '   <-- SAI'}")
    print(f"\n{len(ROLE_CHECKS) - unexpected}/{len(ROLE_CHECKS)} trường hợp đúng như phân quyền")
    return 1 if unexpected else 0


# ---------------------------------------------------------------- lệnh

def cmd_run(args):
    conn = trino_conn(args)
    total = 0.0
    for f in query_files(args.queries):
        cols, rows, secs = run_trino(conn, f.read_text())
        total += secs
        print(f"\n== {f.stem}: {len(rows)} dòng, {secs:.2f}s")
        print_table(cols, rows, args.limit)
    print(f"\nTổng thời gian truy vấn Trino: {total:.1f}s")


def cmd_verify(args):
    conn = trino_conn(args)
    duck = duck_conn(args.data_dir, erased_customers(args))
    failed, skipped = [], []
    for f in query_files(args.queries):
        ind = INDEPENDENT / f.name
        if not ind.exists():
            skipped.append(f.stem)
            print(f"[bỏ qua] {f.stem}: không có tính toán độc lập ({ind.relative_to(REPO)})")
            continue
        sql_text = f.read_text()
        t_cols, t_rows, t_s = run_trino(conn, sql_text)
        d_cols, d_rows, d_s = run_duck(duck, ind.read_text())
        issues = compare(keys_of(sql_text), t_cols, t_rows, d_cols, d_rows)
        status = "KHỚP" if not issues else "LỆCH"
        print(f"[{status}] {f.stem}: {len(t_rows)} dòng × {len(t_cols)} cột (trino {t_s:.2f}s, duckdb {d_s:.2f}s)")
        for msg in issues[:10]:
            print(f"    {msg}")
        if len(issues) > 10:
            print(f"    … {len(issues) - 10} chỗ lệch nữa")
        if issues:
            failed.append(f.stem)
    n = len(query_files(args.queries)) - len(skipped)
    print(f"\n{n - len(failed)}/{n} truy vấn khớp với tính toán độc lập"
          + (f"; lệch: {', '.join(failed)}" if failed else "")
          + (f"; bỏ qua: {', '.join(skipped)}" if skipped else ""))
    return 1 if failed else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=8082)
    ap.add_argument("--user", default="analyst", help="user Trino (mặc định %(default)s, vai trò analyst)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="chạy truy vấn trên Trino, in kết quả")
    r.add_argument("queries", nargs="*", help="tiền tố tên truy vấn, VD q03 q05b (mặc định: tất cả)")
    r.add_argument("--limit", type=int, default=40, help="số dòng in tối đa mỗi truy vấn")
    v = sub.add_parser("verify", help="so kết quả Trino với DuckDB trên CSV gốc")
    v.add_argument("queries", nargs="*")
    v.add_argument("--data-dir", default=str(REPO / "data" / "raw"))
    sub.add_parser("roles", help="thử đọc/ghi bằng user của từng vai trò, so với phân quyền mong đợi")
    args = ap.parse_args()
    return {"run": cmd_run, "verify": cmd_verify, "roles": cmd_roles}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
