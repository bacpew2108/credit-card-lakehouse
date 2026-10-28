"""Biến đổi Silver → Gold (mục 5.4 Design Doc, docs/data_model.md mục 3).

- Dimension: khóa thay thế ổn định (assign_keys) và hai dòng đặc biệt −1 Unknown, −2 Deleted
  (special_rows).
- Fact: một dòng là một giao dịch; khóa của mọi dimension được tra bằng join broadcast trên khóa
  tự nhiên, không khớp thì −1.
- Mart: tổng hợp theo tháng từ fact join dimension (star join).

Toàn bộ là biểu thức Spark gốc, không dùng UDF, nên pytest chạy được trên máy host.
"""

import warnings
from functools import reduce
from operator import and_

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from transform import silver_transforms as st

UNKNOWN, DELETED = -1, -2
SPECIAL = ((UNKNOWN, "Unknown"), (DELETED, "Deleted"))
DATE_FIRST, DATE_LAST = "2010-01-01", "2019-12-31"
DAY_NAMES = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]

# assign_keys đánh số trên một cửa sổ không chia partition: dimension lớn nhất chỉ vài chục nghìn dòng
warnings.filterwarnings("ignore", message="WARN WindowExpression: No Partition Defined")


# ---------------------------------------------------------------- nhóm phân khúc

def age_band(age):
    """Tuổi tại thời điểm giao dịch → nhóm; có giao dịch ở tuổi 17 nên nhóm đầu là '< 18'."""
    return (F.when(age.isNull(), "Unknown").when(age < 18, "< 18").when(age < 25, "18–24")
             .when(age < 35, "25–34").when(age < 45, "35–44").when(age < 55, "45–54")
             .when(age < 65, "55–64").otherwise("65+"))


def income_band(income):
    """yearly_income (USD) → nhóm, ngưỡng chọn theo phân bố của 2.000 khách."""
    return (F.when(income.isNull(), "Unknown").when(income < 30000, "< 30k")
             .when(income < 40000, "30–40k").when(income < 50000, "40–50k")
             .when(income < 75000, "50–75k").otherwise("≥ 75k"))


def credit_score_band(score):
    """credit_score → nhóm theo thang FICO."""
    return (F.when(score.isNull(), "Unknown").when(score < 580, "Poor (< 580)")
             .when(score < 670, "Fair (580–669)").when(score < 740, "Good (670–739)")
             .when(score < 800, "Very Good (740–799)").otherwise("Exceptional (≥ 800)"))


def error_combo(errors):
    """['Insufficient Balance', 'Bad PIN'] → 'Bad PIN, Insufficient Balance'; mảng rỗng → 'None'."""
    return F.when(F.size(errors) > 0, F.array_join(F.array_sort(errors), ", ")).otherwise(F.lit("None"))


# ---------------------------------------------------------------- khóa thay thế, dòng đặc biệt

def assign_keys(src, current, key, nk):
    """Gắn khóa thay thế cho src (có các cột nk, chưa có key) theo dim hiện có current.

    NK đã có trong current giữ key cũ; NK mới nhận key tiếp theo sau key lớn nhất, theo thứ tự NK
    trong lượt này. Vì vậy key phụ thuộc thứ tự nạp: dựng cả khoảng một lần thì theo thứ tự NK,
    nạp từng tháng thì NK xuất hiện sớm hơn nhận key nhỏ hơn. So khớp NK null-safe (vị trí online có
    state, zip NULL). src phải không trùng NK.
    """
    cur = current.where(F.col(key) > 0).select(key, *[F.col(c).alias(f"_k_{c}") for c in nk])
    same = reduce(and_, [F.col(c).eqNullSafe(F.col(f"_k_{c}")) for c in nk])
    joined = src.join(cur, same, "left").drop(*[f"_k_{c}" for c in nk])
    top = cur.agg(F.max(key)).first()[0] or 0
    order = Window.orderBy(*[F.asc_nulls_first(c) for c in nk])
    new = (joined.where(F.col(key).isNull())
                 .withColumn(key, (F.lit(top) + F.row_number().over(order)).cast("int")))
    return joined.where(F.col(key).isNotNull()).unionByName(new)


def special_rows(dim, key):
    """Hai dòng −1 Unknown và −2 Deleted theo schema của dim: cột chuỗi mang nhãn, cột khác NULL."""
    schema = T.StructType([T.StructField(f.name, f.dataType, True) for f in dim.schema.fields])
    rows = [tuple(k if f.name == key else label if isinstance(f.dataType, T.StringType) else None
                  for f in schema.fields)
            for k, label in SPECIAL]
    return dim.sparkSession.createDataFrame(rows, schema)


# ---------------------------------------------------------------- dimension (chưa có khóa)

LOCATION_NK = ["merchant_city", "merchant_state", "zip"]
PROFILE_NK = ["channel", "error_combo", "is_refund"]


def dim_customer(customers):
    """silver.card.customers → dim_customer; không có address, birth_month, current_age."""
    return customers.select(
        "customer_id", "gender", "birth_year", "yearly_income",
        income_band(F.col("yearly_income")).alias("income_band"),
        "total_debt", "debt_to_income", "credit_score",
        credit_score_band(F.col("credit_score")).alias("credit_score_band"),
        "num_credit_cards", "latitude", "longitude")


def dim_card(cards):
    """silver.card.cards → dim_card; không có PAN, CVV, card_bin."""
    return cards.select("card_id", "customer_id", "card_brand", "card_type", "card_token", "card_last4",
                        "has_chip", "credit_limit", "expires_date", "acct_open_date", "num_cards_issued",
                        "year_pin_last_changed")


def dim_merchant(merchants):
    """silver.card.merchants → dim_merchant: vị trí chính của merchant (chuỗi cửa hàng)."""
    return merchants.select("merchant_id", "merchant_city", "merchant_state", "zip", "is_online", "is_foreign")


def dim_mcc(mcc_codes):
    return mcc_codes.select("mcc_code", "description", "mcc_group")


def locations(txn):
    """Các vị trí giao dịch (city, state, zip) khác nhau trong giao dịch Silver."""
    return (txn.select(*LOCATION_NK).distinct()
               .withColumn("is_online", F.coalesce(F.col("merchant_city") == "ONLINE", F.lit(False)))
               .withColumn("is_foreign", st.is_foreign(F.col("merchant_state"))))


def txn_profiles(txn):
    """Các tổ hợp (kênh, tổ hợp lỗi, hoàn tiền) khác nhau trong giao dịch Silver (junk dimension)."""
    return (txn.select("channel", error_combo(F.col("error_list")).alias("error_combo"), "is_refund")
               .distinct()
               .withColumn("has_error", F.col("error_combo") != "None"))


def dim_date(spark, first=DATE_FIRST, last=DATE_LAST):
    """Lịch từ first đến last; date_key = yyyymmdd, day_of_week 1 = thứ Hai."""
    d = F.col("full_date")
    dow = (F.dayofweek(d) + 5) % 7 + 1          # Spark: Chủ Nhật = 1
    return (spark.range(1)
            .select(F.explode(F.sequence(F.lit(first).cast("date"), F.lit(last).cast("date"))).alias("full_date"))
            .select(F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"), d,
                    dow.alias("day_of_week"),
                    F.element_at(F.array(*[F.lit(n) for n in DAY_NAMES]), dow).alias("day_name"),
                    F.weekofyear(d).alias("week_of_year"), F.month(d).alias("month"),
                    F.quarter(d).alias("quarter"), F.year(d).alias("year"),
                    (dow >= 6).alias("is_weekend")))


# ---------------------------------------------------------------- fact

KEYS = ["date_key", "customer_key", "card_key", "merchant_key", "location_key", "mcc_key", "txn_profile_key"]
FACT_COLS = ["transaction_id", *KEYS, "transaction_ts", "txn_hour", "customer_age_at_txn", "amount", "batch_id"]
# dimension: (khóa, [(cột của giao dịch, cột khóa tự nhiên của dim)])
LOOKUPS = {
    "date": ("date_key", [("_date_key", "date_key")]),
    "customer": ("customer_key", [("customer_id", "customer_id")]),
    "card": ("card_key", [("card_id", "card_id")]),
    "merchant": ("merchant_key", [("merchant_id", "merchant_id")]),
    "location": ("location_key", [(c, c) for c in LOCATION_NK]),
    "mcc": ("mcc_key", [("mcc_code", "mcc_code")]),
    "txn_profile": ("txn_profile_key", [("channel", "channel"), ("_error_combo", "error_combo"),
                                        ("is_refund", "is_refund")]),
}


def fact(txn, dims):
    """Giao dịch Silver (có _batch_id) + dims {tên: DataFrame dimension} → các cột FACT_COLS.

    Mỗi khóa được tra bằng join broadcast trên khóa tự nhiên (null-safe, bỏ qua dòng −1, −2);
    không khớp thì −1 (R04, R06). customer_age_at_txn = năm giao dịch − birth_year.
    """
    df = (txn.withColumn("_date_key", F.date_format("transaction_ts", "yyyyMMdd").cast("int"))
             .withColumn("_error_combo", error_combo(F.col("error_list"))))
    for name, (key, on) in LOOKUPS.items():
        extra = [F.col("birth_year").alias("_birth_year")] if name == "customer" else []
        d = dims[name].where(F.col(key) > 0).select(
            key, *[F.col(dc).alias(f"_{name}_{dc}") for _, dc in on], *extra)
        same = reduce(and_, [F.col(tc).eqNullSafe(F.col(f"_{name}_{dc}")) for tc, dc in on])
        df = df.join(F.broadcast(d), same, "left").drop(*[f"_{name}_{dc}" for _, dc in on])
    return df.select(
        "transaction_id",
        *[F.coalesce(F.col(k), F.lit(UNKNOWN)).alias(k) for k in KEYS],
        "transaction_ts",
        F.hour("transaction_ts").alias("txn_hour"),
        (F.year("transaction_ts") - F.col("_birth_year")).alias("customer_age_at_txn"),
        "amount",
        F.col("_batch_id").alias("batch_id"))


# ---------------------------------------------------------------- mart

def _month():
    return F.trunc(F.to_date("transaction_ts"), "month").alias("month")


def _with(f, dims, name, key, cols):
    """fact join một dimension (gồm cả dòng −1, −2) để lấy thêm các cột cols."""
    return f.join(F.broadcast(dims[name].select(key, *cols)), key, "left")


def _txn_count():
    return F.count(F.lit(1)).alias("txn_count")


def _sum(c, name, where=None):
    v = F.col(c) if where is None else F.when(where, F.col(c))
    return F.coalesce(F.sum(v), F.lit(0)).cast("decimal(18,2)").alias(name)


def _known(key):
    """Khóa của dòng thật (bỏ −1, −2) để đếm khách hàng, thẻ khác nhau."""
    return F.when(F.col(key) > 0, F.col(key))


def mart_monthly_kpi(f, dims):
    p = _with(f, dims, "txn_profile", "txn_profile_key", ["channel", "has_error", "is_refund"])
    return p.groupBy(_month()).agg(
        _txn_count(), _sum("amount", "total_amount"),
        F.avg("amount").cast("decimal(12,2)").alias("avg_amount"),
        F.count_distinct(_known("customer_key")).alias("active_customers"),
        F.count_distinct(_known("card_key")).alias("active_cards"),
        F.count_if(F.col("is_refund")).alias("refund_count"),
        _sum("amount", "refund_amount", F.col("is_refund")),
        F.count_if(F.col("has_error")).alias("error_txn_count"),
        *[F.count_if(F.col("channel") == c).alias(f"{c.lower()}_count") for c in ("Swipe", "Chip", "Online")])


def mart_mcc_monthly(f, dims):
    p = _with(_with(f, dims, "mcc", "mcc_key", ["mcc_code", "mcc_group", "description"]),
              dims, "txn_profile", "txn_profile_key", ["is_refund"])
    return p.groupBy(_month(), "mcc_code", "mcc_group", "description").agg(
        _txn_count(), _sum("amount", "total_amount"),
        F.count_if(F.col("is_refund")).alias("refund_count"),
        _sum("amount", "refund_amount", F.col("is_refund")))


def mart_geo_monthly(f, dims):
    """Theo vị trí thật của giao dịch (dim_location), không theo vị trí chính của merchant."""
    p = _with(f, dims, "location", "location_key", ["merchant_state", "is_online", "is_foreign"])
    return p.groupBy(_month(), "merchant_state", "is_online", "is_foreign").agg(
        _txn_count(), _sum("amount", "total_amount"))


def mart_error_monthly(f, dims):
    """Mỗi loại lỗi (tách từ error_combo) × kênh; channel_txn_count là mẫu số để tính tỷ lệ lỗi."""
    p = _with(f, dims, "txn_profile", "txn_profile_key", ["channel", "error_combo"])
    totals = p.groupBy(_month(), "channel").agg(F.count(F.lit(1)).alias("channel_txn_count"))
    errors = (p.where(~F.col("error_combo").isin("None", "Unknown", "Deleted"))
               .select(_month(), "channel", F.explode(F.split("error_combo", ", ")).alias("error_type"))
               .groupBy("month", "error_type", "channel").agg(F.count(F.lit(1)).alias("error_count")))
    return errors.join(totals, ["month", "channel"]).select(
        "month", "error_type", "channel", "error_count", "channel_txn_count")


def mart_customer_segment_monthly(f, dims):
    p = _with(f, dims, "customer", "customer_key", ["income_band", "credit_score_band"])
    m = p.groupBy(_month(), age_band(F.col("customer_age_at_txn")).alias("age_band"),
                  "income_band", "credit_score_band").agg(
        F.count_distinct(_known("customer_key")).alias("customer_count"),
        _txn_count(), _sum("amount", "total_amount"))
    return m.withColumn("avg_spend_per_customer",
                        F.when(F.col("customer_count") > 0, F.col("total_amount") / F.col("customer_count"))
                         .cast("decimal(18,2)"))


def mart_card_monthly(f, dims):
    """Mỗi thẻ mỗi tháng. utilization = tổng giao dịch mua (amount > 0) / credit_limit, chỉ cho thẻ
    Credit có credit_limit > 0; thẻ khác NULL."""
    p = _with(f, dims, "card", "card_key", ["card_brand", "card_type", "credit_limit"])
    m = p.groupBy(_month(), "card_key", "card_brand", "card_type", "credit_limit").agg(
        _txn_count(), _sum("amount", "total_amount"),
        _sum("amount", "purchase_amount", F.col("amount") > 0))
    credit = (F.col("card_type") == "Credit") & (F.col("credit_limit") > 0)
    return m.withColumn("utilization",
                        F.when(credit, F.col("purchase_amount") / F.col("credit_limit")).cast("decimal(10,4)"))


# mart: (hàm dựng, [(cột, kiểu)]) theo thứ tự cột trong bảng
MARTS = {
    "monthly_kpi": (mart_monthly_kpi, [
        ("month", "DATE"), ("txn_count", "BIGINT"), ("total_amount", "DECIMAL(18,2)"),
        ("avg_amount", "DECIMAL(12,2)"), ("active_customers", "BIGINT"), ("active_cards", "BIGINT"),
        ("refund_count", "BIGINT"), ("refund_amount", "DECIMAL(18,2)"), ("error_txn_count", "BIGINT"),
        ("swipe_count", "BIGINT"), ("chip_count", "BIGINT"), ("online_count", "BIGINT")]),
    "mcc_monthly": (mart_mcc_monthly, [
        ("month", "DATE"), ("mcc_code", "INT"), ("mcc_group", "STRING"), ("description", "STRING"),
        ("txn_count", "BIGINT"), ("total_amount", "DECIMAL(18,2)"), ("refund_count", "BIGINT"),
        ("refund_amount", "DECIMAL(18,2)")]),
    "geo_monthly": (mart_geo_monthly, [
        ("month", "DATE"), ("merchant_state", "STRING"), ("is_online", "BOOLEAN"), ("is_foreign", "BOOLEAN"),
        ("txn_count", "BIGINT"), ("total_amount", "DECIMAL(18,2)")]),
    "error_monthly": (mart_error_monthly, [
        ("month", "DATE"), ("error_type", "STRING"), ("channel", "STRING"), ("error_count", "BIGINT"),
        ("channel_txn_count", "BIGINT")]),
    "customer_segment_monthly": (mart_customer_segment_monthly, [
        ("month", "DATE"), ("age_band", "STRING"), ("income_band", "STRING"), ("credit_score_band", "STRING"),
        ("customer_count", "BIGINT"), ("txn_count", "BIGINT"), ("total_amount", "DECIMAL(18,2)"),
        ("avg_spend_per_customer", "DECIMAL(18,2)")]),
    "card_monthly": (mart_card_monthly, [
        ("month", "DATE"), ("card_key", "INT"), ("card_brand", "STRING"), ("card_type", "STRING"),
        ("credit_limit", "DECIMAL(12,2)"), ("txn_count", "BIGINT"), ("total_amount", "DECIMAL(18,2)"),
        ("purchase_amount", "DECIMAL(18,2)"), ("utilization", "DECIMAL(10,4)")]),
}
