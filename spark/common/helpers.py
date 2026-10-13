"""Hàm thuần cho lớp Bronze: không import pyspark, nên pytest chạy được trên máy host."""

import hmac
import re
from datetime import date, datetime, timezone
from hashlib import sha256

MONTH_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")

# Cột nghiệp vụ của từng bảng Bronze, theo đúng thứ tự trong bảng (docs/data_model.md mục 1)
TXN_COLS = [
    "id", "date", "client_id", "card_id", "amount", "use_chip", "merchant_id",
    "merchant_city", "merchant_state", "zip", "mcc", "errors",
]
CUSTOMER_COLS = [
    "id", "current_age", "retirement_age", "birth_year", "birth_month", "gender", "address",
    "latitude", "longitude", "per_capita_income", "yearly_income", "total_debt",
    "credit_score", "num_credit_cards",
]
# cards ở nguồn: lấy card_number để token hóa, KHÔNG lấy cvv
CARD_SRC_COLS = [
    "id", "client_id", "card_brand", "card_type", "card_number", "expires", "has_chip",
    "num_cards_issued", "credit_limit", "acct_open_date", "year_pin_last_changed",
]
CARD_COLS = [
    "id", "client_id", "card_brand", "card_type", "card_token", "card_bin", "card_last4",
    "expires", "has_chip", "num_cards_issued", "credit_limit", "acct_open_date",
    "year_pin_last_changed",
]
MCC_COLS = ["mcc", "description"]

# Bronze giữ mọi cột ở dạng chuỗi: ép ::text ngay trong câu truy vấn nguồn
CORE_CARD_TEXT_COLS = {"transactions": TXN_COLS, "customers": CUSTOMER_COLS, "cards": CARD_SRC_COLS}


def parse_month(month):
    """'2019-03' → date(2019, 3, 1); sai định dạng thì báo lỗi."""
    m = MONTH_RE.match(month or "")
    if not m:
        raise ValueError(f"tháng phải có dạng YYYY-MM, nhận được {month!r}")
    return date(int(m[1]), int(m[2]), 1)


def next_month(d):
    return date(d.year + d.month // 12, d.month % 12 + 1, 1)


def month_bounds(month):
    """Khoảng [đầu tháng M, đầu tháng M+1) dạng 'YYYY-MM-DD'."""
    start = parse_month(month)
    return start.isoformat(), next_month(start).isoformat()


def iter_months(first, last):
    """Danh sách tháng từ first đến last (tính cả hai đầu)."""
    d, end = parse_month(first), parse_month(last)
    if d > end:
        raise ValueError(f"{first} sau {last}")
    out = []
    while d <= end:
        out.append(f"{d:%Y-%m}")
        d = next_month(d)
    return out


def select_text_sql(table, where=""):
    """SELECT các cột nghiệp vụ của bảng core_card, mọi cột ép sang text."""
    cols = ", ".join(f"{c}::text AS {c}" for c in CORE_CARD_TEXT_COLS[table])
    return f"SELECT {cols} FROM {table}" + (f" WHERE {where}" if where else "")


def month_filter(month):
    """Điều kiện lọc giao dịch tháng M (dùng index transactions_date_idx)."""
    start, end = month_bounds(month)  # đã kiểm tra định dạng nên ghép chuỗi an toàn
    return f"date >= TIMESTAMP '{start}' AND date < TIMESTAMP '{end}'"


def ref_batch_id(day=None):
    """_batch_id của dữ liệu tham chiếu: ref_YYYYMMDD (mặc định là hôm nay, UTC)."""
    day = day or datetime.now(timezone.utc).date()
    return f"ref_{day:%Y%m%d}"


def require_secret(secret):
    if not secret or secret == "change_me":
        raise ValueError("HMAC_SECRET chưa được đặt trong .env (make secrets)")
    return secret


def protect_pan(card_number, secret):
    """PAN → (card_token, card_bin, card_last4).

    card_token = hex(HMAC-SHA256(secret, card_number)), 64 ký tự. Khóa là chuỗi HMAC_SECRET
    nguyên văn (UTF-8). Đổi khóa thì toàn bộ token đổi theo.
    """
    if card_number is None:
        return None, None, None
    pan = card_number.strip()
    token = hmac.new(secret.encode(), pan.encode(), sha256).hexdigest()
    return token, pan[:6], pan[-4:]
