# Tuần 7 – Gold

Demo cuối mốc: star schema truy vấn được.
- `gold.card` có 7 dimension (mỗi bảng thêm dòng −1 Unknown, −2 Deleted), `fact_transaction` với 13.305.915 dòng, và 6 mart cho 118 tháng.
- Fact không có khóa −1 nào, vì R04 và R06 trên dữ liệu thật bằng 0.
- R11 (Silver = fact = `mart_monthly_kpi`) PASS ở cả 118 tháng.
- `dag_monthly` chạy đủ 8 task, từ kiểm tra nguồn đến đối soát Gold.
- Mart tháng 2019-02 khớp với số tính độc lập bằng DuckDB trên CSV gốc: KPI, top bang, số giao dịch nước ngoài, utilization.

## Sản phẩm

| Sản phẩm | File |
|---|---|
| Hàm biến đổi Gold: nhóm phân khúc, khóa thay thế, dimension, tra khóa cho fact, 6 mart (biểu thức Spark gốc, không UDF) | [spark/transform/gold_transforms.py](../spark/transform/gold_transforms.py) |
| Job dimension: tham chiếu (customer, card, mcc, date) và từ giao dịch (merchant, location, txn_profile) | [spark/transform/gold_dimensions.py](../spark/transform/gold_dimensions.py) |
| Job fact theo tháng, chặn khi có rule BLOCK | [spark/transform/gold_fact.py](../spark/transform/gold_fact.py) |
| Job 6 mart | [spark/transform/marts.py](../spark/transform/marts.py) |
| R11: Silver = fact = mart_monthly_kpi | [spark/quality/reconcile.py](../spark/quality/reconcile.py) (`silver-gold`), [spark/quality/rules.py](../spark/quality/rules.py) (`R11`, `SILVER_RULES`, `blocked_batches`) |
| DAG: `gold_build`, `refresh_marts`, `reconcile_silver_gold` trong `dag_monthly`; `gold_dimensions` trong `dag_reference` | [airflow/dags/dag_monthly.py](../airflow/dags/dag_monthly.py), [airflow/dags/dag_reference.py](../airflow/dags/dag_reference.py) |
| Kiểm thử (10 test mới, tổng 65); fixture DQ dùng chung | [tests/test_gold_transforms.py](../tests/test_gold_transforms.py), [tests/conftest.py](../tests/conftest.py) |
| Lệnh `gold-ref`, `gold`, `gold-range`, `gold-status` | [Makefile](../Makefile) |

## Cách dùng

```bash
make gold-ref                              # dim_customer, dim_card, dim_mcc, dim_date (sau silver-ref)
make gold MONTH=2019-03                    # dim từ giao dịch → fact → 6 mart → R11
make gold-range FROM=2010-01 TO=2019-10    # như trên cho nhiều tháng, mỗi bước một Spark session
make gold-status                           # số dòng các bảng Gold, số khóa −1, kết quả R11
```

Chạy bằng Airflow (`airflow dags test`; xem lý do ở [week6.md](week6.md)):

```bash
docker compose exec airflow-scheduler airflow dags test dag_monthly 2019-03-15 -c '{"month": "2019-03"}'
docker compose exec airflow-scheduler airflow dags test dag_reference
```

Truy vấn star join qua Trino (`make trino`), ví dụ giờ × thứ trong tuần (Q3):

```sql
SELECT d.day_name, f.txn_hour, count(*) AS n
FROM gold.card.fact_transaction f JOIN gold.card.dim_date d USING (date_key)
WHERE f.transaction_ts >= TIMESTAMP '2019-01-01 00:00:00'
GROUP BY 1, 2 ORDER BY n DESC LIMIT 3;
```

## Cách hoạt động

**Dimension** ([gold_dimensions.py](../spark/transform/gold_dimensions.py)):
- `reference` dựng dim_customer, dim_card, dim_mcc từ Silver tham chiếu và dim_date sinh theo lịch 2010–2019. `transactions` dựng dim_merchant từ `silver.card.merchants`; với dim_location và dim_txn_profile, nó chỉ thêm các vị trí và tổ hợp mới có trong giao dịch của các tháng được dựng.
- Khóa thay thế do `assign_keys` gán: khóa tự nhiên đã có giữ khóa cũ, khóa tự nhiên mới nhận khóa tiếp theo theo thứ tự. So khớp khóa tự nhiên là null-safe, vì vị trí online có state và zip NULL.
- Ghi bằng `lh.merge_changes` như Silver, nên chạy lại khi Silver không đổi thì không ghi dòng nào.

**Fact** ([gold_fact.py](../spark/transform/gold_fact.py)):
1. Dừng nếu một tháng có rule từ nguồn đến Silver (`dq.SILVER_RULES`) đang BLOCK, hoặc dimension tham chiếu còn rỗng.
2. Cập nhật các dimension lấy từ giao dịch.
3. Với mỗi tháng: join broadcast 7 dimension trên khóa tự nhiên, không khớp thì −1; tính `customer_age_at_txn` = năm giao dịch − birth_year; ghi đè partition của tháng bằng `overwritePartitions`.

**Mart** ([marts.py](../spark/transform/marts.py)):
- Tổng hợp từ fact join dimension (gồm cả dòng −1, −2, nên khóa không khớp hiện là `Unknown`).
- Cả khoảng tháng được tính trong một lượt, và mỗi mart chỉ ghi một lần.
- `mart_geo_monthly` lấy vị trí từ dim_location.
- `mart_error_monthly` tách `error_combo` thành từng loại lỗi.

**R11** ([reconcile.py](../spark/quality/reconcile.py) `silver-gold`):
- Mỗi bảng (Silver, fact, mart_monthly_kpi) được tổng hợp theo tháng một lần cho cả khoảng.
- Mỗi tháng so 4 cặp: số dòng và tổng amount của Silver với fact, và của fact với mart.
- Lệch thì BLOCK và task `reconcile_silver_gold` thất bại.

## Quyết định thiết kế

1. **Thêm `dim_location`, `dim_merchant` chỉ giữ vị trí chính.** `merchant_id` là một chuỗi cửa hàng: 21,6% merchant có nhiều vị trí, có merchant ở 4.403 vị trí. Đo trên dữ liệu gốc, 61% giao dịch (8,1 triệu) không diễn ra ở vị trí chính và 58% khác cả bang. Nếu lấy vị trí qua dim_merchant thì Q5 sai cho hơn nửa số giao dịch. Vị trí giao dịch vì vậy là một dimension riêng gồm 25.425 bộ (city, state, zip), và fact có cả `merchant_key` lẫn `location_key` (Hình 3, Hình 4, mục 5.4 Design Doc).
2. **Nhóm thu nhập chọn theo phân bố:** `< 30k`, `30–40k`, `40–50k`, `50–75k`, `≥ 75k`, tương ứng 340 / 617 / 448 / 454 / 141 khách. Với ngưỡng nháp (`30–50k`, `75–100k`, `≥ 100k`), 53% khách dồn vào một nhóm còn hai nhóm trên cùng chỉ có 89 và 52 khách. Điểm tín dụng giữ các nhóm FICO, vì phân bố 81 / 348 / 931 / 474 / 166 đã đủ đều và là thang quen thuộc.
3. **Khóa thay thế ổn định thay vì đánh số lại mỗi lần.** Nếu đánh số theo thứ tự khóa tự nhiên trên toàn bảng, một vị trí mới xuất hiện ở tháng sau sẽ đẩy lệch khóa của mọi vị trí đứng sau nó, và fact của các tháng cũ trỏ sai. Giữ khóa cũ, chỉ cấp khóa mới cho khóa tự nhiên mới, thì fact cũ luôn đúng. Đổi lại, khóa phụ thuộc thứ tự nạp: dựng cả khoảng tháng một lần thì khóa theo thứ tự khóa tự nhiên, nạp từng tháng thì theo tháng xuất hiện đầu tiên. Vì vậy chỉ dựng lại một dimension từ đầu khi dựng lại cả fact.
4. **Mart partition theo `month` và ghi đè bằng điều kiện trên `month`, dù mart nhỏ.** Iceberg chỉ ghi đè theo điều kiện được khi điều kiện khớp trọn file hoặc trọn partition. Với mart không partition, sau khi compaction ở tuần 8 gộp nhiều tháng vào một file, phép ghi đè một tháng sẽ lỗi. Partition theo tháng sinh 118 partition cho mỗi mart, nhưng điều kiện trên `month` luôn khớp trọn partition. Không dùng `overwritePartitions`, vì nó chỉ thay các partition có trong dữ liệu mới: một tháng không còn dòng nào của một mart (VD `mart_error_monthly` của tháng không có lỗi) sẽ giữ số cũ.
5. **`dq_silver` và job Gold chỉ xét các rule từ nguồn đến Silver (`SILVER_RULES`).** Nếu xét cả R11 thì một lần R11 BLOCK cũ sẽ làm `dq_silver` thất bại mỗi lần chạy lại `dag_monthly`, nên Gold không bao giờ được dựng lại để sửa lỗi đó.
6. **R11 so cả `mart_monthly_kpi`, không chỉ fact.** Mart là thứ dashboard đọc trực tiếp, nên đối soát fact = mart bắt được lỗi ở bước tổng hợp mà Silver = fact không thấy.

## Lưu ý

- **utilization có thể lớn hơn 1.** Ví dụ thẻ 2704 tháng 2019-02 chi 2.411,55 trên hạn mức 300, utilization 8,04. Đây là đặc điểm của dữ liệu giả lập. Q9 nên xem phân bố hoặc trung vị, không chỉ trung bình.
- **Thuộc tính khách đổi thì mart cũ không tự tính lại.** dim_customer là SCD loại 1, nên khi `dag_reference` cập nhật thuộc tính của khách (ví dụ thu nhập), `mart_customer_segment_monthly` của các tháng cũ vẫn giữ nhóm cũ cho tới khi chạy lại `marts.py --from … --to …`. `dag_rebuild` ở tuần 8 sẽ lo việc này.
- **Fact tạo snapshot mới mỗi lần chạy lại một tháng** (`overwritePartitions`), dù dữ liệu không đổi. Dimension thì không. `expire_snapshots` nằm trong `dag_maintenance` ở tuần 8.
- Chưa có `publish_run_report`, `on_failure_callback` (tuần 8), truy vấn Q1–Q10 và dashboard (tuần 9).
