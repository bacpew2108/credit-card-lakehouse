"""Tạo kết nối Trino, dataset, chart và dashboard D1–D4 của Superset từ code (make dashboards).

    docker compose exec -T superset python /app/lakehouse/dashboards.py          # import
    docker compose exec -T superset python /app/lakehouse/dashboards.py check    # chạy thử truy vấn mọi chart

Mỗi dataset là một truy vấn trong sql/queries (dataset ảo, SQL lấy nguyên từ file), nên dashboard hiển
thị đúng kết quả đã được đối chiếu bằng `make verify-queries`. Mọi đối tượng có uuid cố định (uuid5
theo tên), import bằng ImportAssetsCommand với chế độ ghi đè: chạy lại thì cập nhật tại chỗ, không tạo
bản sao. Sau khi import, cột của từng dataset được lấy lại từ Trino (fetch_metadata), nên Gold phải có
dữ liệu trước khi chạy.

Superset kết nối Trino bằng user TRINO_USER (mặc định superset), thuộc nhóm analyst: chỉ đọc catalog
gold, ops (infra/trino/rules.json).
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
# Trong container: thư mục mount ở LAKEHOUSE_QUERIES; trên host (pytest): sql/queries của repo
QUERIES = Path(os.environ.get("LAKEHOUSE_QUERIES", HERE.parent.parent / "sql" / "queries"))
NS = uuid.UUID("6f1c9a52-2d0e-4b8a-9c39-4f6f3c1d7a10")
VERSION = "1.0.0"


def uid(name):
    return str(uuid.uuid5(NS, name))


DATABASE = {
    "database_name": "Lakehouse (Trino)",
    "sqlalchemy_uri": f"trino://{os.environ.get('TRINO_USER', 'superset')}@trino:8080/gold",
    "cache_timeout": None,
    "expose_in_sqllab": True,
    "allow_run_async": False,
    "allow_ctas": False,
    "allow_cvas": False,
    "allow_dml": False,
    "allow_csv_upload": False,
    "extra": {"allows_virtual_table_explore": True},
    "uuid": uid("database"),
    "version": VERSION,
}


# ---------------------------------------------------------------- dataset

def metric(name, label, expression, fmt=",.0f"):
    return {"metric_name": name, "verbose_name": label, "expression": expression, "d3format": fmt,
            "metric_type": None, "description": None, "warning_text": None, "extra": {}}


def calc(name, expression, label=None, col_type="VARCHAR"):
    """Cột tính toán của dataset (nhãn để sắp xếp, nhãn hiển thị); kiểu DATE là cột thời gian."""
    return {"column_name": name, "verbose_name": label, "expression": expression, "type": col_type,
            "is_dttm": col_type == "DATE", "is_active": True, "groupby": True, "filterable": True, "extra": {}}


SUM_TXN = metric("sum_txn", "Số giao dịch", "SUM(txn_count)")
SUM_AMOUNT = metric("sum_amount", "Tổng giá trị (USD)", "SUM(total_amount)")
AVG_AMOUNT = metric("avg_amount", "Giá trị TB mỗi giao dịch (USD)",
                    "CAST(SUM(total_amount) AS double) / SUM(txn_count)", ",.2f")
YEAR_LABEL = calc("year_label", "CAST(year AS varchar)", "Năm")
SEGMENT_LABEL = """CASE segment
    WHEN '< 18' THEN '0–17' WHEN '< 30k' THEN '0–30k'
    WHEN 'Poor (< 580)' THEN '300–579 (Poor)' WHEN 'Fair (580–669)' THEN '580–669 (Fair)'
    WHEN 'Good (670–739)' THEN '670–739 (Good)' WHEN 'Very Good (740–799)' THEN '740–799 (Very Good)'
    WHEN 'Exceptional (≥ 800)' THEN '800–850 (Exceptional)' ELSE segment END"""


def max_of(col, label, fmt=",.2f"):
    """Truy vấn đã tính sẵn tỷ lệ, mỗi nhóm một dòng: metric chỉ lấy lại giá trị đó."""
    return metric(f"max_{col}", label, f"MAX({col})", fmt)


DATASETS = {
    "q01a_monthly_trend": {"main_dttm_col": "month", "metrics": [
        SUM_TXN, SUM_AMOUNT, AVG_AMOUNT, metric("month_count", "Số tháng", "COUNT(*)"),
        max_of("avg_amount", "Giá trị TB mỗi giao dịch (USD)")]},
    "q01b_seasonality": {"metrics": [max_of("avg_index", "Chỉ số mùa vụ TB", ".3f")],
                         "columns": [calc("month_label", "lpad(CAST(month_of_year AS varchar), 2, '0')", "Tháng")]},
    "q02_channel_mix": {"metrics": [SUM_TXN], "columns": [YEAR_LABEL]},
    "q03_hour_weekday": {"metrics": [SUM_TXN, SUM_AMOUNT], "columns": [
        calc("day_label", "CAST(day_of_week AS varchar) || '. ' || day_name", "Ngày trong tuần"),
        calc("hour_label", "lpad(CAST(txn_hour AS varchar), 2, '0') || 'h'", "Giờ")]},
    "q04a_mcc_group": {"metrics": [SUM_TXN, SUM_AMOUNT, AVG_AMOUNT]},
    "q04b_top_mcc": {"metrics": [SUM_AMOUNT]},
    "q05a_state": {"metrics": [SUM_TXN, SUM_AMOUNT]},
    "q05b_top_city": {"metrics": [SUM_TXN],
                      "columns": [calc("city_label", "merchant_city || ', ' || merchant_state", "Thành phố")]},
    "q05c_online_foreign": {"metrics": [max_of("pct_online", "Online (%)"),
                                        max_of("pct_foreign", "Nước ngoài (%)", ",.3f")],
                            "columns": [YEAR_LABEL]},
    "q06a_error_rate": {"metrics": [max_of("pct_error", "Tỷ lệ lỗi (%)", ",.3f"),
                                    metric("sum_error", "Số giao dịch lỗi", "SUM(error_count)")]},
    "q06b_refund_by_group": {"metrics": [max_of("pct_refund", "Tỷ lệ hoàn tiền (%)")]},
    # Nhãn nhóm của mart ('< 18', 'Poor (< 580)'…) xếp theo bảng mã không đúng thứ tự: segment_label
    # đổi sang nhãn bắt đầu bằng khoảng giá trị để thứ tự chữ trùng thứ tự nhóm
    "q07_segment_spend": {"metrics": [
        metric("avg_spend", "Chi tiêu TB mỗi khách mỗi tháng (USD)",
               "CAST(SUM(total_amount) AS double) / SUM(customer_months)", ",.2f"),
        metric("sum_customer_months", "Số khách-tháng", "SUM(customer_months)")],
        "columns": [calc("segment_label", SEGMENT_LABEL, "Nhóm")]},
    "q08_card_brand_type": {"metrics": [
        SUM_TXN, SUM_AMOUNT, metric("sum_cards", "Số thẻ có giao dịch", "SUM(active_cards)"),
        metric("pct_credit_amount", "Tỷ trọng giá trị của thẻ Credit (%)",
               "100 * CAST(SUM(CASE WHEN card_type = 'Credit' THEN total_amount ELSE 0 END) AS double)"
               " / SUM(total_amount)", ".2f")]},
    "q09_utilization": {"metrics": [max_of("pct_card_months", "Tỷ lệ thẻ-tháng (%)"),
                                    metric("pct_over_limit", "Thẻ-tháng vượt hạn mức (%)",
                                           "MAX(CASE WHEN bucket_order = 6 THEN pct_card_months END)", ".2f")],
                        "columns": [calc("bucket_label", "CAST(bucket_order AS varchar) || '. ' || bucket",
                                         "Mức sử dụng hạn mức")]},
    "q10a_batches": {"main_dttm_col": "batch_month", "metrics": [
        metric("batch_count", "Số batch tháng", "COUNT(*)"),
        metric("sum_ingested", "Số dòng nạp vào Bronze", "SUM(ingested_rows)"),
        metric("sum_quarantine", "Số bản ghi bị cách ly", "SUM(quarantine_rows)"),
        metric("sum_warn", "Số rule WARN", "SUM(dq_warn)"),
        metric("block_batches", "Số batch có rule BLOCK", "SUM(CASE WHEN dq_block > 0 THEN 1 ELSE 0 END)")],
        "columns": [calc("batch_month", "CAST(date_parse(batch_id || '-01', '%Y-%m-%d') AS date)", "Tháng",
                         "DATE")]},
    "q10b_rule_violations": {"metrics": [metric("sum_violations", "Số vi phạm", "SUM(violations)")]},
    "q10c_pipeline_runs": {"metrics": [metric("sum_runs", "Số lần chạy", "SUM(runs)"),
                                       max_of("avg_seconds", "Thời gian TB mỗi lần chạy (giây)", ",.0f")],
                           "columns": [calc("run_label", "dag_id || ' (' || status || ')'", "DAG (kết quả)")]},
}


def dataset_config(name, spec):
    sql_text = (QUERIES / f"{name}.sql").read_text()
    first = sql_text.splitlines()[0].lstrip("- ").strip()
    return {
        "table_name": name,
        "main_dttm_col": spec.get("main_dttm_col"),
        "description": first,
        "default_endpoint": None,
        "offset": 0,
        "cache_timeout": None,
        "catalog": "gold",
        "schema": "card",
        "sql": sql_text,
        "params": None,
        "template_params": None,
        "filter_select_enabled": True,
        "fetch_values_predicate": None,
        "extra": None,
        "normalize_columns": False,
        "always_filter_main_dttm": False,
        "uuid": uid(f"dataset:{name}"),
        "metrics": spec.get("metrics", []),
        "columns": spec.get("columns", []),
        "version": VERSION,
        "database_uuid": DATABASE["uuid"],
    }


# ---------------------------------------------------------------- chart

NO_FILTER = {"time_range": "No filter", "adhoc_filters": []}


def where(col, value):
    """Bộ lọc đơn giản col = value của một chart (VD Q7: chỉ một loại phân khúc)."""
    return {"clause": "WHERE", "expressionType": "SIMPLE", "subject": col, "operator": "==",
            "comparator": value, "filterOptionName": f"filter_{col}_{value}"}


def chart(key, title, dataset, viz_type, params, description=None, filters=()):
    p = {"viz_type": viz_type, **NO_FILTER, **params}
    p["adhoc_filters"] = p["adhoc_filters"] + list(filters)
    return {"slice_name": title, "description": description, "viz_type": viz_type, "params": p,
            "query_context": None, "cache_timeout": None, "uuid": uid(f"chart:{key}"), "version": VERSION,
            "dataset_uuid": uid(f"dataset:{dataset}"), "_key": key}


def big_number(key, title, dataset, m, subheader, fmt="SMART_NUMBER", filters=()):
    return chart(key, title, dataset, "big_number_total",
                 {"metric": m, "subheader": subheader, "header_font_size": 0.4,
                  "subheader_font_size": 0.15, "y_axis_format": fmt}, filters=filters)


def line(key, title, dataset, x, metrics, fmt="SMART_NUMBER", temporal=False, time_format="%Y", **extra):
    params = {"x_axis": x, "metrics": metrics, "groupby": [], "row_limit": 10000, "show_legend": len(metrics) > 1,
              "legendOrientation": "top", "rich_tooltip": True, "y_axis_format": fmt, "markerEnabled": True,
              "x_axis_sort_asc": True, "truncateYAxis": False, **extra}
    if temporal:
        params.update({"time_grain_sqla": "P1M", "x_axis_time_format": time_format,
                       "adhoc_filters": [{"clause": "WHERE", "comparator": "No filter", "expressionType": "SIMPLE",
                                          "operator": "TEMPORAL_RANGE", "subject": x}]})
    return chart(key, title, dataset, "echarts_timeseries_line", params)


def bar(key, title, dataset, x, metrics, groupby=(), fmt="SMART_NUMBER", horizontal=False, stack=False,
        percent=False, sort_by_value=False, filters=(), **extra):
    params = {"x_axis": x, "metrics": metrics, "groupby": list(groupby), "row_limit": 10000,
              "orientation": "horizontal" if horizontal else "vertical", "show_legend": bool(groupby),
              "legendOrientation": "top", "rich_tooltip": True, "y_axis_format": fmt, "show_value": not groupby,
              "stack": "Stack" if stack else None, "truncateYAxis": False, **extra}
    if percent:
        params.update({"contributionMode": "row", "y_axis_format": ".0%"})
    if sort_by_value:
        params.update({"x_axis_sort": metrics[0], "x_axis_sort_asc": horizontal})
    else:
        params.update({"x_axis_sort_asc": True})
    return chart(key, title, dataset, "echarts_timeseries_bar", params, filters=filters)


def labeled(dataset, name):
    """Metric của dataset dưới dạng metric SQL có nhãn: heatmap hiện tên metric (sum_txn) ở chú giải,
    không dùng verbose_name của dataset."""
    m = next(m for m in DATASETS[dataset]["metrics"] if m["metric_name"] == name)
    return {"expressionType": "SQL", "sqlExpression": m["expression"], "label": m["verbose_name"],
            "hasCustomLabel": True, "optionName": f"metric_{name}"}


def heatmap(key, title, dataset, x, y, m, fmt="SMART_NUMBER", y_sort="alpha_asc"):
    """y_sort: trục y của ECharts vẽ phần tử đầu ở dưới cùng, nên alpha_desc đặt nhãn nhỏ nhất lên trên."""
    return chart(key, title, dataset, "heatmap_v2",
                 {"x_axis": x, "groupby": y, "metric": labeled(dataset, m), "row_limit": 10000,
                  "sort_x_axis": "alpha_asc", "sort_y_axis": y_sort, "normalize_across": "heatmap",
                  "linear_color_scheme": "superset_seq_1",
                  "show_legend": True, "show_percentage": False, "show_values": False, "y_axis_format": fmt,
                  "value_bounds": [None, None], "xscale_interval": -1, "yscale_interval": -1})


def pie(key, title, dataset, groupby, m):
    return chart(key, title, dataset, "pie",
                 {"groupby": [groupby], "metric": m, "row_limit": 100, "sort_by_metric": True, "donut": True,
                  "innerRadius": 40, "outerRadius": 75, "show_labels": True, "labels_outside": True,
                  "label_line": True, "label_type": "key_percent", "number_format": "SMART_NUMBER",
                  "show_legend": False, "color_scheme": "supersetColors"})


def table(key, title, dataset, columns, order_by, ascending=False, height=None):
    """columns: [(cột, tiêu đề, định dạng số hoặc None)]; height: chiều cao ô (đơn vị lưới), mặc định
    ROW_HEIGHT["table"]."""
    config = {c: {"customColumnName": label, **({"d3NumberFormat": fmt} if fmt else {})} for c, label, fmt in columns}
    return chart(key, title, dataset, "table",
                 {"query_mode": "raw", "all_columns": [c for c, _, _ in columns],
                  "order_by_cols": [f'["{order_by}", {str(ascending).lower()}]'], "row_limit": 200,
                  "server_pagination": False,
                  "include_search": False, "show_cell_bars": False, "table_timestamp_format": "smart_date",
                  "percent_metrics": [], "metrics": [], "groupby": [], "column_config": config}) | (
        {"_height": height} if height else {})


def treemap(key, title, dataset, groupby, m):
    return chart(key, title, dataset, "treemap_v2",
                 {"groupby": [groupby], "metric": m, "row_limit": 100, "show_labels": True,
                  "show_upper_labels": False, "label_type": "key_value", "number_format": "SMART_NUMBER",
                  "color_scheme": "supersetColors"})


# ---------------------------------------------------------------- dashboard D1, D2

D1_CHARTS = [
    [big_number("d1_txn", "Tổng số giao dịch", "q01a_monthly_trend", "sum_txn", "01/2010 – 10/2019"),
     big_number("d1_amount", "Tổng giá trị", "q01a_monthly_trend", "sum_amount", "USD"),
     big_number("d1_avg", "Giá trị trung bình mỗi giao dịch", "q01a_monthly_trend", "avg_amount", "USD", ",.2f"),
     big_number("d1_months", "Số tháng dữ liệu", "q01a_monthly_trend", "month_count", "tháng", ",d")],
    [line("d1_amount_month", "Q1. Tổng giá trị giao dịch theo tháng (USD)", "q01a_monthly_trend", "month",
          ["sum_amount"], temporal=True),
     line("d1_txn_month", "Q1. Số giao dịch theo tháng", "q01a_monthly_trend", "month", ["sum_txn"],
          temporal=True)],
    [bar("d1_season", "Q1. Chỉ số mùa vụ theo tháng trong năm (2010–2018, 1 = trung bình năm)",
         "q01b_seasonality", "month_label", ["max_avg_index"], fmt=".2f", y_axis_bounds=[0.8, 1.1]),
     bar("d1_channel", "Q2. Cơ cấu kênh giao dịch theo năm (% số giao dịch)", "q02_channel_mix", "year_label",
         ["sum_txn"], groupby=["channel"], stack=True, percent=True)],
    [heatmap("d1_hour_weekday", "Q3. Số giao dịch theo giờ trong ngày × ngày trong tuần", "q03_hour_weekday",
             "hour_label", "day_label", "sum_txn", y_sort="alpha_desc")],
]

D2_CHARTS = [
    [pie("d2_group_share", "Q4. Tỷ trọng giá trị theo nhóm ngành hàng", "q04a_mcc_group", "mcc_group",
         "sum_amount"),
     bar("d2_group_avg", "Q4. Giá trị trung bình mỗi giao dịch theo nhóm ngành (USD)", "q04a_mcc_group",
         "mcc_group", ["avg_amount"], fmt=",.2f", horizontal=True, sort_by_value=True)],
    [table("d2_top_mcc", "Q4. 15 ngành hàng (MCC) có tổng giá trị lớn nhất", "q04b_top_mcc",
           [("mcc_code", "MCC", None), ("description", "Ngành hàng", None), ("mcc_group", "Nhóm", None),
            ("txn_count", "Số giao dịch", ",d"), ("total_amount", "Tổng giá trị (USD)", ",.2f"),
            ("pct_amount", "Tỷ trọng (%)", ".2f"), ("avg_amount", "TB mỗi giao dịch (USD)", ",.2f")],
           "total_amount")],
    [treemap("d2_state", "Q5. Số giao dịch trong nước theo bang (diện tích theo số giao dịch)", "q05a_state",
             "merchant_state", "sum_txn"),
     bar("d2_top_city", "Q5. 20 thành phố có nhiều giao dịch nhất", "q05b_top_city", "city_label", ["sum_txn"],
         horizontal=True, sort_by_value=True)],
    [line("d2_online", "Q5. Tỷ lệ giao dịch online theo năm (%)", "q05c_online_foreign", "year_label",
          ["max_pct_online"], fmt=".2f"),
     line("d2_foreign", "Q5. Tỷ lệ giao dịch ở nước ngoài theo năm (%)", "q05c_online_foreign", "year_label",
          ["max_pct_foreign"], fmt=".3f")],
    [heatmap("d2_error_rate", "Q6. Tỷ lệ lỗi theo loại lỗi × kênh (% số giao dịch của kênh)", "q06a_error_rate",
             "channel", "error_type", "max_pct_error", fmt=".3f"),
     bar("d2_refund", "Q6. Tỷ lệ hoàn tiền (amount < 0) theo nhóm ngành (%)", "q06b_refund_by_group",
         "mcc_group", ["max_pct_refund"], fmt=".2f", horizontal=True, sort_by_value=True)],
]

# ---------------------------------------------------------------- dashboard D3, D4

AGE, INCOME, SCORE = (where("segment_type", t) for t in ("age_band", "income_band", "credit_score_band"))

D3_CHARTS = [
    [big_number("d3_spend", "Chi tiêu TB mỗi khách mỗi tháng", "q07_segment_spend", "avg_spend", "USD", ",.2f",
                filters=[AGE]),
     big_number("d3_cards", "Số thẻ có giao dịch", "q08_card_brand_type", "sum_cards", "thẻ", ",d"),
     big_number("d3_credit", "Tỷ trọng giá trị của thẻ Credit", "q08_card_brand_type", "pct_credit_amount",
                "% tổng giá trị", ".2f"),
     big_number("d3_over_limit", "Thẻ-tháng vượt hạn mức", "q09_utilization", "pct_over_limit",
                "% số thẻ-tháng của thẻ Credit", ".2f")],
    [bar("d3_age", "Q7. Chi tiêu TB mỗi khách mỗi tháng theo nhóm tuổi (USD)", "q07_segment_spend",
         "segment_label", ["avg_spend"], fmt=",.0f", filters=[AGE]),
     bar("d3_income", "Q7. … theo nhóm thu nhập năm (USD)", "q07_segment_spend", "segment_label",
         ["avg_spend"], fmt=",.0f", filters=[INCOME]),
     bar("d3_score", "Q7. … theo nhóm điểm tín dụng (USD)", "q07_segment_spend", "segment_label",
         ["avg_spend"], fmt=",.0f", filters=[SCORE], xAxisLabelRotation=45)],
    [bar("d3_brand_amount", "Q8. Tổng giá trị theo thương hiệu × loại thẻ (USD)", "q08_card_brand_type",
         "card_brand", ["sum_amount"], groupby=["card_type"], stack=True),
     bar("d3_brand_txn", "Q8. Số giao dịch theo thương hiệu × loại thẻ", "q08_card_brand_type", "card_brand",
         ["sum_txn"], groupby=["card_type"], stack=True)],
    [table("d3_brand_table", "Q8. Đóng góp của từng thương hiệu và loại thẻ", "q08_card_brand_type",
           [("card_brand", "Thương hiệu", None), ("card_type", "Loại thẻ", None),
            ("active_cards", "Số thẻ có giao dịch", ",d"), ("txn_count", "Số giao dịch", ",d"),
            ("pct_txn", "% số giao dịch", ".2f"), ("total_amount", "Tổng giá trị (USD)", ",.2f"),
            ("pct_amount", "% giá trị", ".2f")],
           "total_amount", height=44)],
    [bar("d3_util", "Q9. Phân bố mức sử dụng hạn mức của thẻ Credit (% số thẻ-tháng)", "q09_utilization",
         "bucket_label", ["max_pct_card_months"], fmt=".2f"),
     table("d3_util_table", "Q9. Mức sử dụng hạn mức: chi tiêu mua hàng trong tháng / credit_limit",
           "q09_utilization",
           [("bucket", "Mức sử dụng", None), ("card_months", "Số thẻ-tháng", ",d"),
            ("pct_card_months", "% thẻ-tháng", ".2f"), ("avg_util_pct", "Mức sử dụng TB (%)", ".2f")],
           # mức sử dụng TB tăng theo nhóm: sắp theo cột hiển thị (bảng raw chỉ sắp được theo cột có trong bảng)
           "avg_util_pct", ascending=True)],
]

D4_CHARTS = [
    [big_number("d4_batches", "Batch tháng đã nạp", "q10a_batches", "batch_count", "01/2010 – 10/2019", ",d"),
     big_number("d4_ingested", "Dòng nạp vào Bronze", "q10a_batches", "sum_ingested", "giao dịch"),
     big_number("d4_quarantine", "Bản ghi bị cách ly", "q10a_batches", "sum_quarantine", "R01–R03", ",d"),
     big_number("d4_block", "Batch có rule BLOCK", "q10a_batches", "block_batches", "theo kết quả mới nhất", ",d")],
    [line("d4_ingested_month", "Q10. Số dòng nạp vào Bronze mỗi batch tháng", "q10a_batches", "batch_month",
          ["sum_ingested"], temporal=True),
     line("d4_warn_month", "Q10. Số rule WARN mỗi batch tháng", "q10a_batches", "batch_month", ["sum_warn"],
          temporal=True)],
    [bar("d4_rules", "Q10. Số vi phạm theo rule (mọi batch tháng)", "q10b_rule_violations", "rule_id",
         ["sum_violations"], fmt=",d", horizontal=True, sort_by_value=True),
     table("d4_rules_table", "Q10. Kết quả rule theo kết quả mới nhất của từng tháng", "q10b_rule_violations",
           [("rule_id", "Rule", None), ("severity", "Mức", None), ("months", "Số tháng kiểm tra", ",d"),
            ("months_violated", "Số tháng có vi phạm", ",d"), ("violations", "Số vi phạm", ",d"),
            ("months_block", "Số tháng BLOCK", ",d")],
           "violations", height=68)],
    [bar("d4_runs", "Q10. Thời gian TB mỗi lần chạy theo DAG và kết quả (giây)", "q10c_pipeline_runs",
         "run_label", ["max_avg_seconds"], fmt=",.0f", horizontal=True, sort_by_value=True),
     table("d4_runs_table", "Q10. Các lần chạy pipeline (ops.pipeline.pipeline_runs)", "q10c_pipeline_runs",
           [("dag_id", "DAG", None), ("status", "Kết quả", None), ("runs", "Số lần chạy", ",d"),
            ("months", "Số tháng xử lý", ",d"), ("avg_seconds", "TB (giây)", ",.0f"),
            ("max_seconds", "Lâu nhất (giây)", ",.0f"), ("seconds_per_month", "Giây mỗi tháng", ",.1f")],
           "dag_id", ascending=True)],
]

# chiều cao mỗi hàng (đơn vị lưới của Superset, 8 px)
ROW_HEIGHT = {"big_number_total": 22, "table": 60, "heatmap_v2": 62, "treemap_v2": 62}
DEFAULT_HEIGHT = 52


def dashboard_config(key, title, slug, rows, note):
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    note_row = f"ROW-{key}-note"
    position["GRID_ID"]["children"].append(note_row)
    position[note_row] = {"type": "ROW", "id": note_row, "children": [f"MARKDOWN-{key}"],
                          "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    position[f"MARKDOWN-{key}"] = {"type": "MARKDOWN", "id": f"MARKDOWN-{key}", "children": [],
                                   "parents": ["ROOT_ID", "GRID_ID", note_row],
                                   "meta": {"width": 12, "height": 10, "code": note}}
    for i, row in enumerate(rows, 1):
        row_id = f"ROW-{key}-{i}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                            "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        width = 12 // len(row)
        height = max(c.get("_height") or ROW_HEIGHT.get(c["viz_type"], DEFAULT_HEIGHT) for c in row)
        for c in row:
            cid = f"CHART-{c['_key']}"
            position[row_id]["children"].append(cid)
            position[cid] = {"type": "CHART", "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                             "meta": {"width": width, "height": height, "chartId": 0, "uuid": c["uuid"],
                                      "sliceName": c["slice_name"]}}
    return {
        "dashboard_title": title, "description": None, "css": "", "slug": slug, "certified_by": "",
        "certification_details": "", "published": True, "uuid": uid(f"dashboard:{key}"), "position": position,
        "metadata": {"color_scheme": "supersetColors", "refresh_frequency": 0, "expanded_slices": {},
                     "label_colors": {}, "shared_label_colors": [], "color_scheme_domain": [],
                     "timed_refresh_immune_slices": [], "cross_filters_enabled": False,
                     "native_filter_configuration": [], "default_filters": "{}"},
        "version": VERSION,
    }


DASHBOARDS = [
    ("d1", "D1 – Tổng quan giao dịch", "d1-tong-quan", D1_CHARTS,
     "**Q1–Q3.** Xu hướng theo tháng, tính mùa vụ, cơ cấu kênh và khung giờ giao dịch, 01/2010 – 10/2019. "
     "Nguồn: `gold.card.mart_monthly_kpi`; khung giờ truy vấn thẳng `fact_transaction` × `dim_date`. "
     "Mỗi biểu đồ dùng một truy vấn trong `sql/queries`, đã đối chiếu với tính toán độc lập trên file gốc "
     "(`make verify-queries`)."),
    ("d2", "D2 – Ngành hàng, merchant và địa lý", "d2-nganh-hang-dia-ly", D2_CHARTS,
     "**Q4–Q6.** Ngành hàng (MCC, nhóm MCC), địa lý nơi giao dịch diễn ra (theo `dim_location`, không theo vị "
     "trí chính của merchant), giao dịch online, nước ngoài, lỗi và hoàn tiền. Nguồn: `mart_mcc_monthly`, "
     "`mart_geo_monthly`, `mart_error_monthly`; top thành phố truy vấn `fact_transaction` × `dim_location`."),
    ("d3", "D3 – Khách hàng và thẻ", "d3-khach-hang-the", D3_CHARTS,
     "**Q7–Q9.** Chi tiêu theo phân khúc khách hàng (tuổi tại thời điểm giao dịch, thu nhập, điểm tín dụng), "
     "đóng góp của thương hiệu và loại thẻ, mức sử dụng hạn mức của thẻ Credit. Một khách-tháng là một cặp "
     "(khách, tháng) có giao dịch. Nguồn: `mart_customer_segment_monthly`, `mart_card_monthly`. Khách hàng đã "
     "xóa theo yêu cầu (`make erase`) nằm ở nhóm Deleted."),
    ("d4", "D4 – Chất lượng dữ liệu và vận hành pipeline", "d4-chat-luong-van-hanh", D4_CHARTS,
     "**Q10.** Mỗi batch tháng nạp bao nhiêu dòng, bao nhiêu bản ghi bị cách ly, rule nào vi phạm nhiều nhất "
     "(theo kết quả mới nhất của từng tháng, không tính rule INFO), pipeline chạy mất bao lâu. Nguồn: "
     "`ops.pipeline.ingestion_log`, `quarantine_transactions`, `dq_results`, `pipeline_runs`. R07 (kênh lệch "
     "merchant_city), R14 (ngoài thời hạn thẻ), R12 (02/2019 bất thường) là các vi phạm có thật của dữ liệu gốc."),
]


# ---------------------------------------------------------------- import

def bundle():
    """{đường dẫn trong bundle: nội dung YAML}, cùng cấu trúc với file export của Superset."""
    dump = lambda d: yaml.safe_dump(d, allow_unicode=True, sort_keys=False)  # noqa: E731
    contents = {"metadata.yaml": dump({"version": VERSION, "type": "assets",
                                       "timestamp": datetime.now(timezone.utc).isoformat()}),
                "databases/lakehouse_trino.yaml": dump(DATABASE)}
    for name, spec in DATASETS.items():
        contents[f"datasets/lakehouse_trino/{name}.yaml"] = dump(dataset_config(name, spec))
    for key, title, slug, rows, note in DASHBOARDS:
        for row in rows:
            for c in row:
                c = {k: v for k, v in c.items() if not k.startswith("_")}
                contents[f"charts/{c['uuid']}.yaml"] = dump(c)
        contents[f"dashboards/{key}.yaml"] = dump(dashboard_config(key, title, slug, rows, note))
    return contents


def chart_query(params):
    """Query object tương đương truy vấn dashboard gửi cho một chart: cột nhóm, metric, thứ tự."""
    viz = params["viz_type"]
    metrics = params.get("metrics") or ([params["metric"]] if params.get("metric") else [])
    filters = [{"col": f["subject"], "op": f["operator"], "val": f["comparator"]}
               for f in params.get("adhoc_filters", []) if f["operator"] != "TEMPORAL_RANGE"]
    if viz == "table":
        return {"columns": params["all_columns"], "metrics": [], "row_limit": params["row_limit"],
                "filters": filters, "orderby": [json.loads(params["order_by_cols"][0])]}
    columns = {"big_number_total": [], "pie": params.get("groupby", []), "treemap_v2": params.get("groupby", []),
               "heatmap_v2": [params.get("x_axis"), params.get("groupby")]}.get(
        viz, [params.get("x_axis"), *params.get("groupby", [])])
    return {"columns": [c for c in columns if c], "metrics": metrics, "filters": filters,
            "row_limit": params.get("row_limit", 10000)}


def check(db, SqlaTable):
    """Chạy truy vấn của từng chart qua ChartDataCommand (cùng đường với dashboard, không dùng cache):
    bắt metric, cột sai và đo thời gian. Tổng thời gian chạy tuần tự là cận trên của thời gian dashboard
    chờ dữ liệu, vì trình duyệt gửi các truy vấn song song."""
    from superset.charts.schemas import ChartDataQueryContextSchema
    from superset.commands.chart.data.get_data_command import ChartDataCommand

    bad = 0
    for key, title, _, rows, _ in DASHBOARDS:
        times = []
        for c in (c for row in rows for c in row):
            ds = db.session.query(SqlaTable).filter_by(uuid=uuid.UUID(c["dataset_uuid"])).one()
            payload = {"datasource": {"id": ds.id, "type": "table"}, "queries": [chart_query(c["params"])],
                       "result_type": "full", "result_format": "json", "force": True}
            t0 = time.time()
            try:
                cmd = ChartDataCommand(ChartDataQueryContextSchema().load(payload))
                cmd.validate()
                q = cmd.run()["queries"][0]
                ok, info = q.get("status") == "success" and q.get("rowcount", 0) > 0, f"{q.get('rowcount')} dòng"
            except Exception as e:  # noqa: BLE001
                ok, info = False, str(e).splitlines()[0][:120]
            times.append(time.time() - t0)
            bad += not ok
            print(f"[check] {'OK ' if ok else 'LỖI'} {times[-1]:5.2f}s  {c['slice_name']}: {info}")
        print(f"[check] {title}: {len(times)} chart, chậm nhất {max(times):.2f}s, "
              f"chạy tuần tự {sum(times):.2f}s")
    return bad


def main():
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db, security_manager
        from superset.commands.importers.v1.assets import ImportAssetsCommand
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard
        from superset.utils.core import override_user

        admin = security_manager.find_user(username="admin")
        with override_user(admin):
            if sys.argv[1:] == ["check"]:
                return 1 if check(db, SqlaTable) else 0
            ImportAssetsCommand(bundle()).run()
            failed = []
            for name in DATASETS:
                ds = db.session.query(SqlaTable).filter_by(uuid=uuid.UUID(uid(f"dataset:{name}"))).one()
                try:
                    ds.fetch_metadata()
                    db.session.commit()
                except Exception as e:  # noqa: BLE001 – báo từng dataset, không dừng cả lượt
                    db.session.rollback()
                    failed.append(name)
                    print(f"[superset] {name}: không lấy được cột từ Trino: {e}")
            for key, title, slug, rows, _ in DASHBOARDS:
                d = db.session.query(Dashboard).filter_by(uuid=uuid.UUID(uid(f"dashboard:{key}"))).one()
                n = sum(len(r) for r in rows)
                print(f"[superset] {title}: {n} chart, http://localhost:8088/superset/dashboard/{d.slug}/")
        print(f"[superset] {len(DATASETS)} dataset, {len(DASHBOARDS)} dashboard")
        return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
