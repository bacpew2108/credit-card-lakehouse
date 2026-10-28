"""Biến đổi Bronze → Silver (mục 5.3 Design Doc, docs/data_model.md mục 2).

Toàn bộ là biểu thức Spark gốc, không dùng UDF, nên chạy nhanh và pytest được trên máy host.
Spark 4 bật ANSI mode: ép kiểu sai làm dừng cả job thay vì trả NULL, nên mọi phép parse
đều kiểm tra định dạng bằng regex rồi dùng try_cast.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import helpers as h

MONEY_RE = r"^\$-?[0-9]+(\.[0-9]{1,2})?$"           # '$-77.00', '$29278'
TS_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$"
ZIP_RE = r"^[0-9]{1,5}(\.0)?$"                       # '2840.0', '58523.0'
MONTH_YEAR_RE = r"^(0[1-9]|1[0-2])/[0-9]{4}$"        # '07/1997'
US_STATE_RE = r"^[A-Z]{2}$"


# ---------------------------------------------------------------- hàm cho từng cột

def parse_money(c):
    """'$-77.00' → -77.00 (decimal(12,2)); sai định dạng hoặc tràn số → NULL."""
    c = F.trim(c)
    return F.when(c.rlike(MONEY_RE), F.regexp_replace(c, r"^\$", "").try_cast("decimal(12,2)"))


def parse_ts(c):
    """'2019-03-01 00:01:00' → timestamp_ntz (giờ địa phương, không múi giờ); ngày sai → NULL."""
    c = F.trim(c)
    return F.when(c.rlike(TS_RE), c.try_cast("timestamp_ntz"))


def normalize_zip(c):
    """'2840.0' → '02840'; NULL giữ NULL (giao dịch online, nước ngoài)."""
    c = F.trim(c)
    return F.when(c.rlike(ZIP_RE), F.lpad(F.regexp_extract(c, r"^([0-9]+)", 1), 5, "0"))


def normalize_channel(c):
    """'Swipe Transaction' → 'Swipe'. Giá trị lạ giữ nguyên (đã bỏ hậu tố) để R07 phát hiện."""
    return F.regexp_replace(F.trim(c), r"\s+Transaction$", "")


def split_errors(c):
    """'Bad PIN,Insufficient Balance' → ['Bad PIN', 'Insufficient Balance']; NULL → []."""
    parts = F.filter(F.transform(F.split(F.coalesce(c, F.lit("")), ","), lambda x: F.trim(x)), lambda x: x != "")
    return parts.cast("array<string>")


def is_foreign(state):
    """merchant_state có giá trị nhưng không phải mã bang 2 chữ cái (là tên quốc gia)."""
    return F.coalesce(state.isNotNull() & ~F.trim(state).rlike(US_STATE_RE), F.lit(False))


def month_year_to_date(c, month_end=False):
    """'07/1997' → 1997-07-01, hoặc 1997-07-31 nếu month_end."""
    c = F.trim(c)
    first = F.make_date(F.substring(c, 4, 4).cast("int"), F.substring(c, 1, 2).cast("int"), F.lit(1))
    return F.when(c.rlike(MONTH_YEAR_RE), F.last_day(first) if month_end else first)


def yes_no(c):
    v = F.upper(F.trim(c))
    return F.when(v == "YES", True).when(v == "NO", False)


def normalize_gender(c):
    v = F.lower(F.trim(c))
    return F.when(v.isin("male", "m"), "Male").when(v.isin("female", "f"), "Female")


def normalize_card_type(c):
    v = F.trim(c)
    return F.when(v == "Debit (Prepaid)", "Prepaid").otherwise(v)


def to_num(c, t="int"):
    """Chuỗi số → kiểu t (int, smallint, bigint, double); không phải số → NULL."""
    return F.trim(c).try_cast(t)


# ---------------------------------------------------------------- bảng tham chiếu

CUSTOMER_COLS = [
    "customer_id", "current_age", "retirement_age", "birth_year", "birth_month", "gender",
    "address", "latitude", "longitude", "per_capita_income", "yearly_income", "total_debt",
    "debt_to_income", "credit_score", "num_credit_cards",
]
CARD_COLS = [
    "card_id", "customer_id", "card_brand", "card_type", "card_token", "card_bin", "card_last4",
    "expires_date", "acct_open_date", "has_chip", "num_cards_issued", "credit_limit",
    "year_pin_last_changed",
]
MCC_COLS = ["mcc_code", "description", "mcc_group"]


def customers(b):
    income, debt = parse_money(F.col("yearly_income")), parse_money(F.col("total_debt"))
    return b.select(
        to_num(F.col("id")).alias("customer_id"),
        *[to_num(F.col(c), "smallint").alias(c)
          for c in ("current_age", "retirement_age", "birth_year", "birth_month")],
        normalize_gender(F.col("gender")).alias("gender"),
        F.trim("address").alias("address"),
        to_num(F.col("latitude"), "double").alias("latitude"),
        to_num(F.col("longitude"), "double").alias("longitude"),
        parse_money(F.col("per_capita_income")).alias("per_capita_income"),
        income.alias("yearly_income"),
        debt.alias("total_debt"),
        F.when(income > 0, debt / income).try_cast("decimal(6,3)").alias("debt_to_income"),
        to_num(F.col("credit_score"), "smallint").alias("credit_score"),
        to_num(F.col("num_credit_cards"), "smallint").alias("num_credit_cards"),
    )


def cards(b):
    return b.select(
        to_num(F.col("id")).alias("card_id"),
        to_num(F.col("client_id")).alias("customer_id"),
        F.trim("card_brand").alias("card_brand"),
        normalize_card_type(F.col("card_type")).alias("card_type"),
        "card_token", "card_bin", "card_last4",
        month_year_to_date(F.col("expires"), month_end=True).alias("expires_date"),
        month_year_to_date(F.col("acct_open_date")).alias("acct_open_date"),
        yes_no(F.col("has_chip")).alias("has_chip"),
        to_num(F.col("num_cards_issued"), "smallint").alias("num_cards_issued"),
        parse_money(F.col("credit_limit")).alias("credit_limit"),
        to_num(F.col("year_pin_last_changed"), "smallint").alias("year_pin_last_changed"),
    )


def mcc_codes(b, groups):
    """groups: DataFrame (mcc, mcc_group) từ ref/mcc_groups.csv; mã chưa phân nhóm → 'Chưa phân nhóm'."""
    m = b.select(to_num(F.col("mcc")).alias("mcc_code"), F.trim("description").alias("description"))
    g = groups.select(to_num(F.col("mcc")).alias("mcc_code"), "mcc_group")
    return (m.join(g, "mcc_code", "left")
             .select("mcc_code", "description",
                     F.coalesce("mcc_group", F.lit("Chưa phân nhóm")).alias("mcc_group")))


# ---------------------------------------------------------------- giao dịch

TXN_COLS = [
    "transaction_id", "transaction_ts", "customer_id", "card_id", "amount", "channel",
    "merchant_id", "merchant_city", "merchant_state", "zip", "mcc_code", "error_list",
    "has_error", "is_refund", "is_online", "is_foreign",
]


def _rule(rule_id, reason):
    return F.struct(F.lit(rule_id).alias("r"), F.lit(reason).alias("m"))


def mark_batch(b, month, other_ids=None):
    """Gắn rule BLOCK (mục 6 Design Doc) cho từng dòng của batch Bronze tháng M, trong một lượt.

    Trả về các cột Bronze + _id, _ts, _amount đã parse + _rule (struct r, m; NULL nếu hợp lệ).
    Thứ tự xét: R01 (id rỗng) → R02 (amount, date) → R03 (ngoài tháng) → R01 (id đã có ở tháng
    khác) → R01 (trùng id trong tháng). Với id trùng trong tháng, giữ bản có transaction_ts sớm
    nhất (hòa thì theo giá trị các cột), các bản còn lại bị gắn R01.

    other_ids: DataFrame có cột transaction_id gồm các id đã có trong Silver ở tháng khác; bản
    trong batch bị gắn R01, tức là bản nạp trước được giữ. None thì không xét.
    """
    start, end = (F.lit(d).cast("timestamp_ntz") for d in h.month_bounds(month))
    parsed = b.select("*",
                      to_num(F.col("id"), "bigint").alias("_id"),
                      parse_ts(F.col("date")).alias("_ts"),
                      parse_money(F.col("amount")).alias("_amount"))
    if other_ids is None:
        parsed = parsed.withColumn("_other", F.lit(False))
    else:
        # Chỉ quét cột id của Silver, giữ lại các id có trong batch (batch nhỏ nên broadcast)
        ids = parsed.select("_id").where(F.col("_id").isNotNull()).distinct()
        dup = (other_ids.select(F.col("transaction_id").alias("_id"))
                        .join(F.broadcast(ids), "_id", "left_semi").distinct()
                        .withColumn("_other", F.lit(True)))
        parsed = (parsed.join(F.broadcast(dup), "_id", "left")
                        .withColumn("_other", F.coalesce(F.col("_other"), F.lit(False))))
    rule = (F.when(F.col("_id").isNull(), _rule("R01", "transaction_id rỗng hoặc không phải số"))
             .when(F.col("_amount").isNull(), _rule("R02", "amount không parse được"))
             .when(F.col("_ts").isNull(), _rule("R02", "date không parse được"))
             .when((F.col("_ts") < start) | (F.col("_ts") >= end),
                   _rule("R03", f"transaction_ts ngoài tháng {month}"))
             .when(F.col("_other"), _rule("R01", "transaction_id đã có ở tháng khác")))
    # Chỉ các dòng chưa vi phạm mới "tranh" nhau giữ một transaction_id
    w = Window.partitionBy("_id", F.col("_rule").isNull()).orderBy("_ts", *h.TXN_COLS)
    return (parsed.withColumn("_rule", rule)
                  .withColumn("_rule", F.when(F.col("_rule").isNull() & (F.row_number().over(w) > 1),
                                              _rule("R01", "trùng transaction_id"))
                                        .otherwise(F.col("_rule")))
                  .drop("_other"))


def split_marked(marked, bronze_cols):
    """Kết quả mark_batch → (valid, quarantine). quarantine có các cột Bronze + rule_id, reason."""
    valid = marked.where(F.col("_rule").isNull()).drop("_rule")
    quarantine = (marked.where(F.col("_rule").isNotNull())
                        .select(*bronze_cols, F.col("_rule.r").alias("rule_id"),
                                F.col("_rule.m").alias("reason")))
    return valid, quarantine


def split_batch(b, month, other_ids=None):
    """Tách batch Bronze tháng M thành (valid, quarantine); valid dùng cho transactions()."""
    return split_marked(mark_batch(b, month, other_ids), b.columns)


def transactions(valid):
    """Batch đã qua split_batch → các cột silver.card.transactions (TXN_COLS)."""
    channel = normalize_channel(F.col("use_chip"))
    errors = split_errors(F.col("errors"))
    return valid.select(
        F.col("_id").alias("transaction_id"),
        F.col("_ts").alias("transaction_ts"),
        to_num(F.col("client_id")).alias("customer_id"),
        to_num(F.col("card_id")).alias("card_id"),
        F.col("_amount").alias("amount"),
        channel.alias("channel"),
        to_num(F.col("merchant_id")).alias("merchant_id"),
        F.trim("merchant_city").alias("merchant_city"),
        F.trim("merchant_state").alias("merchant_state"),
        normalize_zip(F.col("zip")).alias("zip"),
        to_num(F.col("mcc")).alias("mcc_code"),
        errors.alias("error_list"),
        (F.size(errors) > 0).alias("has_error"),
        (F.col("_amount") < 0).alias("is_refund"),
        (channel == "Online").alias("is_online"),
        is_foreign(F.col("merchant_state")).alias("is_foreign"),
    )


# ---------------------------------------------------------------- merchants

MERCHANT_COLS = [
    "merchant_id", "merchant_city", "merchant_state", "zip", "is_online", "is_foreign",
    "location_count", "txn_count",
]


def merchants(txn):
    """Từ silver.card.transactions: mỗi merchant lấy vị trí (city, state, zip) có nhiều giao dịch
    nhất; hòa thì lấy vị trí gặp gần nhất, rồi theo thứ tự chữ cái để kết quả cố định."""
    loc = (txn.where(F.col("merchant_id").isNotNull())
              .groupBy("merchant_id", "merchant_city", "merchant_state", "zip")
              .agg(F.count(F.lit(1)).alias("n"), F.max("transaction_ts").alias("last_seen"),
                   F.max("_batch_id").alias("_batch_id")))
    w = Window.partitionBy("merchant_id")
    order = w.orderBy(F.desc("n"), F.desc("last_seen"),
                      F.asc_nulls_last("merchant_city"), F.asc_nulls_last("merchant_state"),
                      F.asc_nulls_last("zip"))
    return (loc.withColumn("location_count", F.count(F.lit(1)).over(w).cast("int"))
               .withColumn("txn_count", F.sum("n").over(w))
               .withColumn("batch", F.max("_batch_id").over(w))
               .withColumn("rn", F.row_number().over(order))
               .where("rn = 1")
               .select("merchant_id", "merchant_city", "merchant_state", "zip",
                       (F.col("merchant_city") == "ONLINE").alias("is_online"),
                       is_foreign(F.col("merchant_state")).alias("is_foreign"),
                       "location_count", "txn_count", F.col("batch").alias("_batch_id")))
