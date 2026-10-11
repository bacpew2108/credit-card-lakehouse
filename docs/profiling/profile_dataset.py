"""Profiling bộ Financial Transactions Dataset (tuần 1).

Đọc mọi cột dạng chuỗi (giống hệ thống nguồn cũ), đo quy mô, định dạng và các
vi phạm dự kiến cho rule DQ ở mục 6, xuất báo cáo Markdown.

    uv run --with duckdb python docs/profiling/profile_dataset.py \
        --data-dir "../Financial Transactions Dataset" \
        --out docs/profiling/profiling_report.md
"""

import argparse
import json
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import duckdb


def luhn_ok(number: str) -> bool:
    digits = [int(c) for c in number]
    checksum = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        checksum += d
    return checksum % 10 == 0


def fmt(v):
    if isinstance(v, (float, Decimal)):
        return f"{v:,.2f}"
    if isinstance(v, int):
        return f"{v:,}"
    return "" if v is None else str(v)


class Report:
    def __init__(self, con):
        self.con = con
        self.lines = []

    def h(self, level, text):
        self.lines += ["", "#" * level + " " + text, ""]

    def p(self, text):
        self.lines.append(text)

    def rows(self, sql):
        return self.con.execute(sql).fetchall()

    def one(self, sql):
        return self.con.execute(sql).fetchone()

    def table(self, sql, headers=None):
        cur = self.con.execute(sql)
        headers = headers or [d[0] for d in cur.description]
        data = cur.fetchall()
        if self.lines and self.lines[-1] != "":
            self.lines.append("")
        self.lines.append("| " + " | ".join(headers) + " |")
        self.lines.append("|" + "---|" * len(headers))
        for r in data:
            self.lines.append("| " + " | ".join(fmt(v) for v in r) + " |")
        self.lines.append("")
        return data


def money(col):
    # "$-77.00" -> -77.00 ; trả NULL nếu không parse được
    return f"TRY_CAST(REPLACE({col}, '$', '') AS DECIMAL(18,2))"


def load(con, data_dir: Path):
    def csv(name):
        path = (data_dir / name).as_posix().replace("'", "''")
        # Khai báo rõ quote: 20k dòng đầu không có trường bọc "…" nên DuckDB đoán sai,
        # trong khi errors nhiều lỗi có dạng "Insufficient Balance,Technical Glitch"
        return (f"read_csv('{path}', header=true, all_varchar=true, "
                f"delim=',', quote='\"', escape='\"')")

    con.execute(f"CREATE TABLE users AS SELECT * FROM {csv('users_data.csv')}")
    con.execute(f"CREATE TABLE cards AS SELECT * FROM {csv('cards_data.csv')}")
    con.execute(f"CREATE TABLE tx AS SELECT * FROM {csv('transactions_data.csv')}")

    mcc = json.loads((data_dir / "mcc_codes.json").read_text())
    con.execute("CREATE TABLE mcc (mcc_code VARCHAR, description VARCHAR)")
    con.executemany("INSERT INTO mcc VALUES (?, ?)", list(mcc.items()))

    # Cột đã ép kiểu để tính toán; bảng gốc vẫn giữ nguyên chuỗi
    con.execute(f"""
        CREATE TABLE txp AS
        SELECT *,
               TRY_CAST(date AS TIMESTAMP)        AS ts,
               {money('amount')}                  AS amount_num,
               strftime(TRY_CAST(date AS TIMESTAMP), '%Y-%m') AS month
        FROM tx
    """)
    return mcc


def profile(data_dir: Path) -> str:
    con = duckdb.connect()
    t0 = time.time()
    mcc = load(con, data_dir)
    load_s = time.time() - t0
    r = Report(con)

    r.p("# Báo cáo profiling dữ liệu – Financial Transactions Dataset")
    r.p("")
    r.p(f"Sinh tự động bởi `docs/profiling/profile_dataset.py` lúc "
        f"{datetime.now():%Y-%m-%d %H:%M}, DuckDB {duckdb.__version__}. "
        f"Mọi cột được đọc dạng chuỗi (`all_varchar`) như hệ thống nguồn cũ; "
        f"thời gian nạp {load_s:.0f} giây.")

    # ---------------------------------------------------------------- 1
    r.h(2, "1. Quy mô")
    sizes = {f.name: f.stat().st_size for f in data_dir.iterdir() if f.is_file()}
    counts = {
        "users_data.csv": r.one("SELECT count(*) FROM users")[0],
        "cards_data.csv": r.one("SELECT count(*) FROM cards")[0],
        "transactions_data.csv": r.one("SELECT count(*) FROM tx")[0],
        "mcc_codes.json": len(mcc),
    }
    target = {
        "users_data.csv": "PostgreSQL core_card.customers",
        "cards_data.csv": "PostgreSQL core_card.cards",
        "transactions_data.csv": "PostgreSQL core_card.transactions (theo tháng)",
        "mcc_codes.json": "landing/reference/mcc_codes/ (JSON Lines)",
    }
    r.p("| File | Số dòng | Dung lượng | Đưa vào |")
    r.p("|---|---|---|---|")
    for name, n in counts.items():
        r.p(f"| {name} | {n:,} | {sizes[name] / 1e6:,.1f} MB | {target[name]} |")
    if "train_fraud_labels.json" in sizes:
        r.p(f"| train_fraud_labels.json | – | {sizes['train_fraud_labels.json'] / 1e6:,.1f} MB "
            f"| Không dùng (nhãn gian lận cho học máy – ngoài phạm vi) |")
    r.p("")

    lo, hi, months = r.one("SELECT min(ts), max(ts), count(DISTINCT month) FROM txp")
    mn, mx, avg, med = r.one("""
        SELECT min(n), max(n), avg(n), median(n)
        FROM (SELECT month, count(*) n FROM txp GROUP BY month)
    """)
    r.p(f"- Khoảng thời gian giao dịch: **{lo} → {hi}**, gồm **{months} tháng**.")
    r.p(f"- Số giao dịch mỗi tháng (= kích thước một batch): nhỏ nhất {mn:,}, lớn nhất {mx:,}, "
        f"trung bình {avg:,.0f}, trung vị {med:,.0f}.")

    r.h(3, "Số giao dịch và tổng giá trị theo năm")
    r.table("""
        SELECT year(ts)::VARCHAR AS "Năm", count(DISTINCT month) AS "Số tháng",
               count(*) AS "Số giao dịch", round(sum(amount_num), 2) AS "Tổng amount ($)",
               round(avg(amount_num), 2) AS "Amount TB ($)"
        FROM txp GROUP BY 1 ORDER BY 1
    """)

    r.h(3, "Các tháng của năm cuối (dùng cho phát hành từng tháng)")
    r.table("""
        SELECT month AS "Tháng", count(*) AS "Số giao dịch",
               round(sum(amount_num), 2) AS "Tổng amount ($)"
        FROM txp WHERE year(ts) = (SELECT max(year(ts)) FROM txp)
        GROUP BY 1 ORDER BY 1
    """)

    # ---------------------------------------------------------------- 2
    r.h(2, "2. Chất lượng theo cột – transactions")
    cols = [c[0] for c in r.rows("DESCRIBE tx")]
    r.p("| Cột | Null/rỗng | Tỷ lệ null | Số giá trị khác nhau | Ví dụ |")
    r.p("|---|---|---|---|---|")
    total = counts["transactions_data.csv"]
    for c in cols:
        nulls, distinct, sample = r.one(f"""
            SELECT count(*) FILTER (WHERE "{c}" IS NULL OR trim("{c}") = ''),
                   count(DISTINCT "{c}"),
                   any_value("{c}") FILTER (WHERE "{c}" IS NOT NULL AND trim("{c}") <> '')
            FROM tx
        """)
        r.p(f"| {c} | {nulls:,} | {nulls / total:.2%} | {distinct:,} | `{sample}` |")
    r.p("")

    r.h(3, "transaction_id (R01)")
    dup, null_id = r.one("""
        SELECT count(*) - count(DISTINCT id), count(*) FILTER (WHERE id IS NULL) FROM tx
    """)
    r.p(f"- id trùng: **{dup:,}**; id null: **{null_id:,}**.")

    r.h(3, "date, amount (R02)")
    bad_ts, bad_amt, neg, zero = r.one("""
        SELECT count(*) FILTER (WHERE ts IS NULL),
               count(*) FILTER (WHERE amount_num IS NULL),
               count(*) FILTER (WHERE amount_num < 0),
               count(*) FILTER (WHERE amount_num = 0)
        FROM txp
    """)
    amin, amax, nofmt = r.one(r"""
        SELECT min(amount_num), max(amount_num),
               count(*) FILTER (WHERE NOT regexp_full_match(amount, '\$-?[0-9]+\.[0-9]{2}'))
        FROM txp
    """)
    r.p(f"- date không parse được: **{bad_ts:,}**; amount không parse được: **{bad_amt:,}**.")
    r.p(f"- amount dạng `$-77.00`; dòng sai mẫu `$[-]d+.dd`: {nofmt:,}. "
        f"Khoảng giá trị: {amin} → {amax}.")
    r.p(f"- amount < 0 (hoàn tiền): **{neg:,}** ({neg / total:.2%}); amount = 0: {zero:,}.")

    r.h(3, "use_chip – kênh giao dịch (R07)")
    r.table("""
        SELECT use_chip AS "Giá trị gốc", count(*) AS "Số giao dịch",
               round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS "%"
        FROM tx GROUP BY 1 ORDER BY 2 DESC
    """)
    r.table("""
        SELECT use_chip AS "Kênh",
               count(*) FILTER (WHERE merchant_city = 'ONLINE') AS "city = ONLINE",
               count(*) FILTER (WHERE merchant_city <> 'ONLINE') AS "city khác ONLINE",
               count(*) FILTER (WHERE merchant_state IS NULL) AS "state null",
               count(*) FILTER (WHERE zip IS NULL) AS "zip null"
        FROM tx GROUP BY 1 ORDER BY 1
    """)
    r07 = r.one("""
        SELECT count(*) FILTER (WHERE use_chip <> 'Online Transaction' AND merchant_city = 'ONLINE'),
               count(*) FILTER (WHERE use_chip = 'Online Transaction' AND merchant_city <> 'ONLINE')
        FROM tx
    """)
    r.p(f"- Vi phạm R07 (Online ⇔ merchant_city = ONLINE): **{r07[0]:,}** giao dịch không phải Online "
        f"nhưng city = ONLINE; {r07[1]:,} giao dịch Online nhưng city khác ONLINE.")
    r.p("- Chuẩn hóa channel: `Swipe Transaction` → Swipe, `Chip Transaction` → Chip, "
        "`Online Transaction` → Online.")

    r.h(3, "Cơ cấu kênh theo năm (Q2)")
    r.table("""
        SELECT year(ts)::VARCHAR AS "Năm",
               round(100.0 * count(*) FILTER (WHERE use_chip = 'Swipe Transaction') / count(*), 1) AS "% Swipe",
               round(100.0 * count(*) FILTER (WHERE use_chip = 'Chip Transaction') / count(*), 1) AS "% Chip",
               round(100.0 * count(*) FILTER (WHERE use_chip = 'Online Transaction') / count(*), 1) AS "% Online"
        FROM txp GROUP BY 1 ORDER BY 1
    """)

    r.h(3, "merchant_state – giao dịch nước ngoài")
    us = r.one("""
        SELECT count(*) FILTER (WHERE length(merchant_state) = 2),
               count(*) FILTER (WHERE length(merchant_state) <> 2),
               count(DISTINCT merchant_state) FILTER (WHERE length(merchant_state) = 2),
               count(DISTINCT merchant_state) FILTER (WHERE length(merchant_state) <> 2)
        FROM tx
    """)
    r.p(f"- Mã bang 2 ký tự: {us[0]:,} giao dịch / {us[2]} giá trị. "
        f"Giá trị khác (tên quốc gia): **{us[1]:,} giao dịch / {us[3]} giá trị** "
        f"→ xác nhận merchant_state chứa tên quốc gia với giao dịch nước ngoài.")
    r.p("")
    r.p("Top 10 giá trị không phải mã bang:")
    r.p("")
    r.table("""
        SELECT merchant_state AS "merchant_state", count(*) AS "Số giao dịch",
               count(*) FILTER (WHERE zip IS NULL) AS "zip null"
        FROM tx WHERE length(merchant_state) <> 2
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """)

    r.h(3, "zip")
    z = r.one(r"""
        SELECT count(*) FILTER (WHERE zip IS NOT NULL),
               count(*) FILTER (WHERE regexp_full_match(zip, '[0-9]+\.0')),
               count(*) FILTER (WHERE length(split_part(zip, '.', 1)) < 5),
               count(*) FILTER (WHERE length(split_part(zip, '.', 1)) > 5)
        FROM tx
    """)
    r.p(f"- zip có giá trị: {z[0]:,}; dạng số thực `ddddd.0`: **{z[1]:,}**.")
    zn = r.one("""
        SELECT count(*) FILTER (WHERE zip IS NULL),
               count(*) FILTER (WHERE zip IS NULL AND (merchant_city = 'ONLINE' OR length(merchant_state) <> 2))
        FROM tx
    """)
    r.p(f"- zip null: {zn[0]:,}, trong đó {zn[1]:,} là giao dịch online hoặc nước ngoài "
        f"→ zip chỉ null khi merchant không ở Mỹ.")
    r.p(f"- Phần nguyên < 5 chữ số (mất số 0 đầu, cần `lpad(…, 5, '0')`): **{z[2]:,}**; > 5 chữ số: {z[3]:,}.")
    r.table("""
        SELECT merchant_state AS "Bang", split_part(zip, '.', 1) AS "zip gốc",
               lpad(split_part(zip, '.', 1), 5, '0') AS "zip chuẩn hóa", count(*) AS "Số giao dịch"
        FROM tx WHERE zip IS NOT NULL AND length(split_part(zip, '.', 1)) < 5
        GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 5
    """)

    r.h(3, "errors")
    e_rows, e_multi, e_max = r.one("""
        SELECT count(*) FILTER (WHERE errors IS NOT NULL),
               count(*) FILTER (WHERE errors LIKE '%,%'),
               max(len(string_split(errors, ',')))
        FROM tx
    """)
    r.p("- Trường errors có nhiều lỗi được bọc trong dấu ngoặc kép (CSV chuẩn RFC 4180); "
        "20 nghìn dòng đầu không có trường nào như vậy nên công cụ tự đoán định dạng dễ đọc sai "
        "→ seed_source.py phải khai báo rõ quote.")
    r.p(f"- Giao dịch có lỗi: **{e_rows:,}** ({e_rows / total:.2%}); có nhiều lỗi ghép bằng dấu phẩy: "
        f"**{e_multi:,}**; nhiều nhất {e_max} lỗi/giao dịch.")
    r.p("")
    r.table("""
        SELECT trim(e) AS "Loại lỗi", count(*) AS "Số lần xuất hiện"
        FROM (SELECT unnest(string_split(errors, ',')) AS e FROM tx WHERE errors IS NOT NULL)
        GROUP BY 1 ORDER BY 2 DESC
    """)
    r.p(f"Số tổ hợp lỗi khác nhau (cho dim_txn_profile): "
        f"**{r.one('SELECT count(DISTINCT errors) FROM tx')[0]}**.")

    r.h(3, "Giờ trong ngày (Q3)")
    r.p(f"- Số giao dịch có giây ≠ 0: {r.one('SELECT count(*) FROM txp WHERE second(ts) <> 0')[0]:,} "
        f"(thời điểm chỉ chính xác đến phút).")

    # ---------------------------------------------------------------- 3
    r.h(2, "3. Toàn vẹn tham chiếu")
    r.table("""
        SELECT 'R04' AS "Rule", 'client_id không có trong users' AS "Kiểm tra",
               count(*) AS "Số giao dịch vi phạm"
        FROM tx t ANTI JOIN users u ON t.client_id = u.id
        UNION ALL
        SELECT 'R04', 'card_id không có trong cards', count(*)
        FROM tx t ANTI JOIN cards c ON t.card_id = c.id
        UNION ALL
        SELECT 'R05', 'Thẻ không thuộc khách hàng (cards.client_id ≠ client_id)', count(*)
        FROM tx t JOIN cards c ON t.card_id = c.id WHERE c.client_id <> t.client_id
        UNION ALL
        SELECT 'R06', 'mcc không có trong mcc_codes', count(*)
        FROM tx t ANTI JOIN mcc m ON t.mcc = m.mcc_code
        UNION ALL
        SELECT 'R08', 'Thẻ has_chip = NO nhưng có giao dịch chip', count(*)
        FROM tx t JOIN cards c ON t.card_id = c.id
        WHERE c.has_chip = 'NO' AND t.use_chip = 'Chip Transaction'
        UNION ALL
        SELECT '–', 'cards.client_id không có trong users', count(*)
        FROM cards c ANTI JOIN users u ON c.client_id = u.id
    """)
    missing_mcc = r.rows("""
        SELECT t.mcc, count(*) FROM tx t ANTI JOIN mcc m ON t.mcc = m.mcc_code
        GROUP BY 1 ORDER BY 2 DESC
    """)
    if missing_mcc:
        r.p("MCC thiếu mô tả: " + ", ".join(f"`{m}` ({n:,})" for m, n in missing_mcc))
    used, unused = r.one("""
        SELECT count(DISTINCT t.mcc), (SELECT count(*) FROM mcc) - count(DISTINCT m.mcc_code)
        FROM tx t LEFT JOIN mcc m ON t.mcc = m.mcc_code
    """)
    r.p(f"- Số MCC xuất hiện trong giao dịch: {used}; số mã trong mcc_codes.json không được dùng: {unused}.")
    users_no_tx, cards_no_tx = r.one("""
        SELECT (SELECT count(*) FROM users u ANTI JOIN tx t ON t.client_id = u.id),
               (SELECT count(*) FROM cards c ANTI JOIN tx t ON t.card_id = c.id)
    """)
    r.p(f"- Khách hàng không có giao dịch: {users_no_tx:,}; thẻ không có giao dịch: {cards_no_tx:,}.")
    win = r.one("""
        SELECT count(*) FILTER (WHERE date_trunc('month', t.ts) < strptime(c.acct_open_date, '%m/%Y')),
               count(*) FILTER (WHERE date_trunc('month', t.ts) > strptime(c.expires, '%m/%Y'))
        FROM txp t JOIN cards c ON t.card_id = c.id
    """)
    r.p(f"- Giao dịch trước tháng mở thẻ (acct_open_date): **{win[0]:,}**; sau tháng hết hạn (expires): "
        f"**{win[1]:,}** → ứng viên rule WARN bổ sung.")

    r.h(3, "Tuổi khách tại thời điểm giao dịch (Q7)")
    r.table("""
        SELECT CASE WHEN age < 18 THEN '< 18' WHEN age < 25 THEN '18–24' WHEN age < 35 THEN '25–34'
                    WHEN age < 45 THEN '35–44' WHEN age < 55 THEN '45–54' WHEN age < 65 THEN '55–64'
                    ELSE '65+' END AS "Nhóm tuổi",
               count(*) AS "Số giao dịch", min(age) AS "Tuổi min", max(age) AS "Tuổi max"
        FROM (SELECT year(t.ts) - TRY_CAST(u.birth_year AS INT) AS age
              FROM txp t JOIN users u ON t.client_id = u.id)
        GROUP BY 1 ORDER BY min(age)
    """)

    # ---------------------------------------------------------------- 4
    r.h(2, "4. Merchant (suy ra silver.merchants)")
    m = r.one("""
        WITH loc AS (
            SELECT merchant_id, count(DISTINCT (merchant_city, merchant_state, coalesce(zip, ''))) n_loc,
                   count(DISTINCT mcc) n_mcc
            FROM tx GROUP BY 1
        )
        SELECT count(*), count(*) FILTER (WHERE n_loc > 1), max(n_loc),
               count(*) FILTER (WHERE n_mcc > 1), max(n_mcc)
        FROM loc
    """)
    r.p(f"- Số merchant: **{m[0]:,}**.")
    r.p(f"- Merchant có nhiều vị trí (city, state, zip): **{m[1]:,}** ({m[1] / m[0]:.1%}), "
        f"nhiều nhất {m[2]:,} vị trí → ghi vào DQ, lấy vị trí xuất hiện nhiều nhất.")
    r.p(f"- Merchant có nhiều MCC: **{m[3]:,}**, nhiều nhất {m[4]} MCC → "
        f"giữ mcc ở fact thay vì ở dim_merchant.")

    # ---------------------------------------------------------------- 5
    r.h(2, "5. users_data.csv (customers)")
    r.table("""
        SELECT count(*) AS "Dòng", count(DISTINCT id) AS "id khác nhau",
               min(TRY_CAST(current_age AS INT)) AS "Tuổi min", max(TRY_CAST(current_age AS INT)) AS "Tuổi max",
               min(TRY_CAST(birth_year AS INT)) AS "birth_year min", max(TRY_CAST(birth_year AS INT)) AS "birth_year max"
        FROM users
    """)
    snap = r.one("""
        SELECT mode(TRY_CAST(birth_year AS INT) + TRY_CAST(current_age AS INT)) FROM users
    """)[0]
    r.p(f"- birth_year + current_age phổ biến nhất = **{snap}** → năm chụp dữ liệu; "
        f"tuổi tại thời điểm giao dịch phải tính từ birth_year.")
    r.table("""
        SELECT gender AS "gender", count(*) AS "Số khách" FROM users GROUP BY 1 ORDER BY 2 DESC
    """)
    r.p("Các cột tiền (định dạng `$1234`):")
    r.p("")
    money_cols = ["per_capita_income", "yearly_income", "total_debt"]
    r.table(" UNION ALL ".join(f"""
        SELECT '{c}' AS "Cột", any_value({c}) AS "Ví dụ",
               count(*) FILTER (WHERE {money(c)} IS NULL) AS "Không parse được",
               min({money(c)}) AS "Min", max({money(c)}) AS "Max",
               count(*) FILTER (WHERE {money(c)} = 0) AS "= 0"
        FROM users""" for c in money_cols))
    cs = r.one("""
        SELECT min(TRY_CAST(credit_score AS INT)), max(TRY_CAST(credit_score AS INT)),
               count(*) FILTER (WHERE TRY_CAST(credit_score AS INT) NOT BETWEEN 300 AND 850)
        FROM users
    """)
    r.p(f"- credit_score: {cs[0]} → {cs[1]}; ngoài [300, 850] (R09): **{cs[2]}**.")
    ncc = r.one("""
        SELECT count(*) FILTER (WHERE TRY_CAST(u.num_credit_cards AS INT) <> coalesce(c.n, 0)),
               count(*) FILTER (WHERE c.n IS NULL)
        FROM users u
        LEFT JOIN (SELECT client_id, count(*) n FROM cards GROUP BY 1) c ON c.client_id = u.id
    """)
    r.p(f"- num_credit_cards khác số thẻ thực tế trong cards (R09): **{ncc[0]:,}** khách; "
        f"khách không có thẻ nào: {ncc[1]:,}.")
    r.p(f"- Giá trị retirement_age, latitude, longitude, address: không null "
        f"({r.one('SELECT count(*) FROM users WHERE address IS NULL OR latitude IS NULL')[0]} dòng thiếu).")

    # ---------------------------------------------------------------- 6
    r.h(2, "6. cards_data.csv (cards)")
    for col in ["card_brand", "card_type", "has_chip", "card_on_dark_web"]:
        r.table(f'SELECT {col} AS "{col}", count(*) AS "Số thẻ" FROM cards GROUP BY 1 ORDER BY 2 DESC')
    fmts = r.one(r"""
        SELECT count(*) FILTER (WHERE NOT regexp_full_match(expires, '[0-9]{2}/[0-9]{4}')),
               count(*) FILTER (WHERE NOT regexp_full_match(acct_open_date, '[0-9]{2}/[0-9]{4}')),
               min(strptime(expires, '%m/%Y')), max(strptime(expires, '%m/%Y')),
               min(strptime(acct_open_date, '%m/%Y')), max(strptime(acct_open_date, '%m/%Y')),
               count(*) FILTER (WHERE strptime(acct_open_date, '%m/%Y') > strptime(expires, '%m/%Y'))
        FROM cards
    """)
    r.p(f"- expires sai dạng MM/YYYY: {fmts[0]}; acct_open_date sai dạng: {fmts[1]}.")
    r.p(f"- expires: {fmts[2]:%m/%Y} → {fmts[3]:%m/%Y}; acct_open_date: {fmts[4]:%m/%Y} → {fmts[5]:%m/%Y}; "
        f"mở sau ngày hết hạn: {fmts[6]}.")
    cl = r.one(f"""
        SELECT any_value(credit_limit), count(*) FILTER (WHERE {money('credit_limit')} IS NULL),
               min({money('credit_limit')}), max({money('credit_limit')}),
               count(*) FILTER (WHERE {money('credit_limit')} = 0)
        FROM cards
    """)
    r.p(f"- credit_limit ví dụ `{cl[0]}`; không parse được: {cl[1]}; khoảng {cl[2]} → {cl[3]}; "
        f"bằng 0: {cl[4]} (cần loại khi tính mức sử dụng hạn mức – Q9).")
    r.table("""
        SELECT card_type AS "card_type", count(*) AS "Số thẻ",
               count(*) FILTER (WHERE TRY_CAST(REPLACE(credit_limit, '$', '') AS DECIMAL) = 0) AS "credit_limit = 0"
        FROM cards GROUP BY 1 ORDER BY 1
    """)

    r.h(3, "Dữ liệu nhạy cảm: card_number, cvv (M5, R13)")
    pans = [row[0] for row in r.rows("SELECT card_number FROM cards")]
    lengths = {}
    for p in pans:
        lengths[len(p)] = lengths.get(len(p), 0) + 1
    luhn = sum(1 for p in pans if p.isdigit() and luhn_ok(p))
    dup_pan = len(pans) - len(set(pans))
    cvv = r.one("SELECT count(DISTINCT length(cvv)), min(length(cvv)), max(length(cvv)) FROM cards")
    r.p(f"- Độ dài card_number: " + ", ".join(f"{k} chữ số: {v:,}" for k, v in sorted(lengths.items())) + ".")
    r.p(f"- Thỏa kiểm tra Luhn: **{luhn:,}/{len(pans):,}** ({luhn / len(pans):.1%}); số thẻ trùng: {dup_pan}.")
    r.p(f"  → **Phần lớn số thẻ là số giả, không thỏa Luhn.** Nếu R13 chỉ quét chuỗi 13–19 chữ số + Luhn "
        f"thì sẽ bỏ sót {len(pans) - luhn:,} số thẻ bị lọt. R13 cần quét mọi chuỗi 15–16 chữ số "
        f"(bỏ điều kiện Luhn), hoặc so khớp HMAC của chuỗi tìm được với tập card_token.")
    r.p(f"- cvv: độ dài {cvv[1]}–{cvv[2]} ký tự; bị loại ngay ở bước trích xuất.")
    bins = r.one("SELECT count(DISTINCT left(card_number, 6)), count(DISTINCT right(card_number, 4)) FROM cards")
    r.p(f"- BIN (6 số đầu) khác nhau: {bins[0]:,}; last4 khác nhau: {bins[1]:,}.")

    # ---------------------------------------------------------------- 7
    r.h(2, "7. mcc_codes.json")
    r.p(f"- JSON object duy nhất `{{\"mcc\": \"mô tả\"}}`, {len(mcc)} mã → cần chuyển sang JSON Lines "
        f"`{{\"mcc_code\": …, \"description\": …}}` trước khi đặt vào bucket landing.")
    r.p("- Mô tả không có nhóm ngành → cần bảng ánh xạ tự xây mcc_group (≈10 nhóm) cho silver.mcc_codes.")
    r.table("SELECT mcc_code, description FROM mcc ORDER BY mcc_code LIMIT 5")

    # ---------------------------------------------------------------- 8
    r.h(2, "8. Biến động theo tháng (cơ sở cho R12 ±3σ)")
    r12 = r.rows("""
        WITH m AS (SELECT month, count(*) n FROM txp GROUP BY 1),
        w AS (
            SELECT month, n,
                   avg(n) OVER win AS mu, stddev_samp(n) OVER win AS sd,
                   count(*) OVER win AS k
            FROM m WINDOW win AS (ORDER BY month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING)
        )
        SELECT month, n, round(mu), round(sd), round((n - mu) / sd, 2)
        FROM w WHERE k = 12 AND abs(n - mu) > 3 * sd ORDER BY month
    """)
    if r12:
        r.p("Tháng có số giao dịch lệch quá ±3σ so với 12 tháng trước (R12 sẽ cảnh báo):")
        r.p("")
        r.p("| Tháng | Số giao dịch | TB 12 tháng trước | σ | z |")
        r.p("|---|---|---|---|---|")
        for row in r12:
            r.p("| " + " | ".join(fmt(v) for v in row) + " |")
        r.p("")
        r.p("Số giao dịch tăng dần theo năm nên ±3σ trên cửa sổ trượt dễ báo động giả ở các năm đầu; "
            "cân nhắc so sánh theo tăng trưởng hoặc chỉ cảnh báo (WARN) như thiết kế.")
    else:
        r.p("Không có tháng nào lệch quá ±3σ so với 12 tháng trước.")

    r.lines.append("")
    r.p(f"_Tổng thời gian profiling: {time.time() - t0:.0f} giây._")
    return "\n".join(r.lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("../Financial Transactions Dataset"))
    ap.add_argument("--out", type=Path, default=Path("docs/profiling/profiling_report.md"))
    args = ap.parse_args()
    report = profile(args.data_dir)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(report, encoding="utf-8")
    print(f"Đã ghi {args.out}")


if __name__ == "__main__":
    main()
