# Tuần 9 – SQL phân tích, BI (1)

Demo cuối mốc: 2 dashboard.
- 17 truy vấn trong `sql/queries` trả lời Q1–Q10 (mục 2.3 Design Doc). Cả 17 chạy bằng user của vai trò analyst, tổng cộng 9 giây trên Trino.
- `make verify-queries` khớp 16/16 truy vấn với tính toán độc lập. Phía độc lập dùng DuckDB, đọc thẳng file CSV, JSON gốc và tự làm sạch, không đi qua Bronze, Silver, Gold. Số đếm và tổng tiền khớp tuyệt đối. Số đã làm tròn khớp tới chữ số cuối. Riêng `q10c` (thời gian chạy pipeline) không có nguồn độc lập, vì dữ liệu vận hành chỉ có trong `ops`.
- Q10 cho đúng số vi phạm đã biết từ profiling: R07 có 5.788 giao dịch, R14 có 392 giao dịch, R12 có một tháng bất thường (2019-02). Phía độc lập tính lại cả ba con số này từ file gốc.
- Trino có 2 vai trò. `make roles` thử 13 thao tác đọc/ghi bằng engineer, analyst và một user ngoài nhóm, cả 13 đều đúng như phân quyền: analyst đọc được `gold`, `ops`, bị chặn đọc `silver`, `bronze` và bị chặn mọi lệnh ghi.
- `make dashboards` dựng từ code kết nối Trino, 11 dataset và 2 dashboard D1, D2 (mỗi dashboard 9 chart). Chạy lại thì cập nhật tại chỗ, không tạo bản sao. Bước kiểm tra chạy truy vấn của cả 18 chart qua đúng đường dữ liệu của dashboard, và cả 18 chart hiển thị đúng khi mở trên trình duyệt.
- Mở dashboard trên Chrome, không dùng cache: từ lúc mở trang đến khi dữ liệu của cả 9 chart về đủ, D1 mất 2,3–3,3 giây, D2 mất 2,7–3,2 giây (3 lần đo mỗi dashboard), dưới mục tiêu 5 giây. Chart chậm nhất là Q3 và top thành phố (1,7–2,6 giây), vì hai truy vấn này quét fact 13,3 triệu dòng.

## Sản phẩm

| Sản phẩm | File |
|---|---|
| 17 truy vấn Trino cho Q1–Q10, mỗi file ghi câu hỏi, nguồn, dashboard và cột khóa | [sql/queries/](../sql/queries) |
| Cùng câu hỏi tính độc lập bằng DuckDB trên file gốc; bảng dùng chung (làm sạch, vi phạm rule theo tháng) | [sql/independent/](../sql/independent), [_base.sql](../sql/independent/_base.sql) |
| Chạy truy vấn, đối chiếu hai engine, kiểm tra phân quyền | [sql/run_queries.py](../sql/run_queries.py) |
| Phân quyền Trino 2 vai trò theo nhóm | [infra/trino/rules.json](../infra/trino/rules.json), [groups.txt](../infra/trino/groups.txt), [group-provider.properties](../infra/trino/group-provider.properties) |
| Dataset, chart, dashboard D1, D2 dựng từ code; kiểm tra truy vấn của từng chart | [infra/superset/dashboards.py](../infra/superset/dashboards.py) |
| Mount script và `sql/queries` vào Superset | [docker-compose.yml](../docker-compose.yml) |
| Kiểm thử (26 test mới, tổng 125) | [tests/test_analytics.py](../tests/test_analytics.py) |
| Lệnh `queries`, `verify-queries`, `roles`, `dashboards` | [Makefile](../Makefile) |

## Cách dùng

Khi Gold đã có dữ liệu ([tuần 8](week8.md)):

```bash
make queries                     # chạy cả 17 truy vấn, in kết quả và thời gian
make queries Q="q03 q05b"        # chỉ các truy vấn có tên bắt đầu như vậy
make verify-queries              # đối chiếu với DuckDB trên data/raw (khoảng 30 giây)
make roles                       # 13 thao tác đọc/ghi theo từng vai trò
make dashboards                  # import vào Superset rồi chạy thử truy vấn mọi chart
```

Mở Superset ở http://localhost:8088, đăng nhập `admin` bằng `SUPERSET_ADMIN_PASSWORD`, rồi vào Dashboards:
- D1 – Tổng quan giao dịch: http://localhost:8088/superset/dashboard/d1-tong-quan/
- D2 – Ngành hàng, merchant và địa lý: http://localhost:8088/superset/dashboard/d2-nganh-hang-dia-ly/

Sửa một truy vấn trong `sql/queries` thì chạy `make verify-queries` để đối chiếu lại, rồi `make dashboards` để dataset nhận SQL mới.

## Cách hoạt động

**Truy vấn** (`sql/queries`, chạy bằng user `analyst`):

| Câu hỏi | Truy vấn | Nguồn | Kết quả chính |
|---|---|---|---|
| Q1 | `q01a_monthly_trend`, `q01b_seasonality` | `mart_monthly_kpi` | Mùa vụ yếu. Chỉ số mùa vụ (tổng giá trị tháng / trung bình tháng của năm) chạy theo số ngày trong tháng: tháng 31 ngày khoảng 1,02, tháng 30 ngày khoảng 0,99, tháng 2 là 0,92 |
| Q2 | `q02_channel_mix` | `mart_monthly_kpi` | Trước 2015 không có giao dịch Chip. Từ 2015, Chip chiếm khoảng 71%, Swipe giảm từ 88% xuống 17%, Online ổn định ở 11–12% |
| Q3 | `q03_hour_weekday` | `fact_transaction` × `dim_date` | 76,5% giao dịch diễn ra trong khung 6–17 giờ, cao nhất lúc 11–12 giờ (mỗi giờ 7,1%), thấp nhất lúc 2–4 giờ (dưới 0,9%). Các ngày trong tuần gần như đều nhau (14,2–14,4%) |
| Q4 | `q04a_mcc_group`, `q04b_top_mcc` | `mart_mcc_monthly` | Nhóm "Dịch vụ, tiện ích" chiếm 24,6% giá trị. MCC lớn nhất là Money Transfer (9,3%) |
| Q5 | `q05a_state`, `q05b_top_city`, `q05c_online_foreign` | `mart_geo_monthly`; top thành phố từ `fact_transaction` × `dim_location` | CA 12,3%, TX 8,7%, NY 7,4% số giao dịch trong nước; Houston đứng đầu về số giao dịch. Online tăng từ 10,9% (2010) lên khoảng 12,2% từ 2015; nước ngoài 0,58–0,85% |
| Q6 | `q06a_error_rate`, `q06b_refund_by_group` | `mart_error_monthly`, `mart_monthly_kpi`, `mart_mcc_monthly` | Lỗi phổ biến nhất là Insufficient Balance (khoảng 1% ở Swipe, Chip). Lỗi CVV, số thẻ, ngày hết hạn gần như chỉ có ở Online (Swipe có 3 lỗi CVV, 2 lỗi số thẻ). Hoàn tiền cao nhất ở "Công nghiệp, sản xuất" (16,6%) |
| Q7 | `q07_segment_spend` | `mart_customer_segment_monthly` | Chi tiêu trung bình mỗi khách mỗi tháng theo nhóm tuổi, thu nhập, điểm tín dụng (dashboard D3, tuần 10) |
| Q8 | `q08_card_brand_type` | `mart_card_monthly` | Mastercard Debit chiếm 39,6% số giao dịch |
| Q9 | `q09_utilization` | `mart_card_monthly` | 81% cặp (thẻ tín dụng, tháng) dùng dưới 30% hạn mức; 1,7% vượt hạn mức |
| Q10 | `q10a_batches`, `q10b_rule_violations`, `q10c_pipeline_runs` | `ops.pipeline.*`, `mart_monthly_kpi` | 118 batch, không bản ghi nào bị cách ly; R07, R14 là rule vi phạm nhiều nhất; thời gian chạy theo DAG (dashboard D4, tuần 10) |

**Đối chiếu** (`make verify-queries`):
1. DuckDB nạp `transactions_data.csv`, `users_data.csv`, `cards_data.csv`, `mcc_codes.json` vào bộ nhớ (6 giây), tự làm sạch theo [data_model.md](data_model.md): bỏ `$`, đổi kênh, ngày mở thẻ, ngày hết hạn, thẻ `Debit (Prepaid)`, cờ online và nước ngoài. Bảng `tx_ok` là các giao dịch qua được R01–R03. Bảng `dq_month` tính lại vi phạm R01–R08, R12, R14 của từng tháng theo mô tả rule.
2. Mỗi truy vấn trong `sql/queries` có một truy vấn cùng tên trong `sql/independent`, trả về cùng tên cột. Hai kết quả được ghép theo cột khóa khai báo ở dòng `-- khóa:`.
3. Số nguyên và decimal (số đếm, tổng tiền) phải bằng nhau tuyệt đối. Số thực đã làm tròn (tỷ lệ, trung bình) được lệch một đơn vị ở chữ số cuối. Thiếu dòng ở một bên cũng tính là lệch.

**Phân quyền** (file-based access control của Trino):

| Nhóm | User | Catalog | Bảng |
|---|---|---|---|
| `engineer` | `trino` (user mặc định của `make trino` và các lệnh `*-status`), `engineer` | Mọi catalog | Đọc, ghi, tạo, xóa. Riêng `bronze`, `silver`, `gold` chỉ đọc vì catalog đặt `iceberg.security=read_only` |
| `analyst` | `superset` (tài khoản Superset), `analyst` (`make queries`) | `gold`, `ops`, `system`, chỉ đọc | `SELECT` trên `gold.card`, `ops.pipeline` |
| Không thuộc nhóm nào | | Không có | Không có |

**Dashboard** ([dashboards.py](../infra/superset/dashboards.py)):
1. Mỗi dataset là một truy vấn trong `sql/queries`: dataset ảo, SQL đọc nguyên văn từ file. Dataset có thêm metric (VD `SUM(txn_count)`) và cột tính toán: nhãn năm, giờ, ngày để sắp xếp, nhãn thành phố.
2. Script dựng một bundle cùng định dạng file export của Superset (kết nối, dataset, chart, dashboard), rồi import bằng `ImportAssetsCommand` với chế độ ghi đè. Mọi đối tượng có uuid cố định (uuid5 theo tên), nên chạy lại thì cập nhật tại chỗ. Sau khi import, script lấy lại cột của từng dataset từ Trino.
3. `dashboards.py check` gửi truy vấn của từng chart qua `ChartDataCommand`, cùng đường mà dashboard dùng và không dùng cache. Bước này bắt metric hoặc cột sai, và đo thời gian. Đối tượng đã bỏ khỏi script (chart, dataset) không bị import xóa đi, phải xóa trong Superset.

| Dashboard | Chart |
|---|---|
| D1 – Tổng quan giao dịch | 4 số tổng (giao dịch, giá trị, giá trị trung bình, số tháng); tổng giá trị và số giao dịch theo tháng (Q1); chỉ số mùa vụ theo tháng trong năm (Q1); cơ cấu kênh theo năm, cột chồng 100% (Q2); heatmap giờ × ngày trong tuần (Q3) |
| D2 – Ngành hàng, merchant và địa lý | Tỷ trọng giá trị theo nhóm ngành, giá trị trung bình theo nhóm ngành, bảng 15 MCC lớn nhất (Q4); treemap 52 bang theo số giao dịch, 20 thành phố, tỷ lệ online, tỷ lệ nước ngoài theo năm (Q5); heatmap tỷ lệ lỗi theo loại lỗi × kênh, tỷ lệ hoàn tiền theo nhóm ngành (Q6) |

## Quyết định thiết kế

1. **Dashboard đọc đúng file truy vấn đã đối chiếu.** Nếu dựng chart trên bảng mart rồi để Superset tự tổng hợp, con số trên dashboard sẽ đi qua một đường khác với đường đã kiểm chứng. Dùng file `sql/queries` làm dataset ảo thì truy vấn được đối chiếu chính là truy vấn mà dashboard chạy.
2. **Đối chiếu bằng DuckDB trên file gốc, không trên Bronze hay Silver.** So Gold với Silver chỉ chứng minh hai tầng khớp nhau, điều R11 đã làm. Tính lại từ file gốc bằng một engine khác, với code làm sạch viết lại độc lập, thì bắt được cả lỗi ở extract, Silver, mart lẫn lỗi trong truy vấn.
3. **Truy vấn tính tỷ lệ, trung bình từ tổng, không dùng cột đã làm tròn của mart.** `avg_amount` và `utilization` của mart đã làm tròn, nên tính lại từ `total_amount / txn_count`, `purchase_amount / credit_limit`. Nhờ đó phép so khớp tuyệt đối có ý nghĩa, và Q9 không xếp nhầm nhóm ở biên (VD 0,09996 làm tròn thành 0,1000).
4. **Q3 và top thành phố truy vấn thẳng fact.** Đúng như Design Doc (Q3 không có mart), và mart theo bang không có thành phố. Hai truy vấn này quét 13,3 triệu dòng mất khoảng 1–2 giây, vẫn trong mục tiêu 5 giây, nên chưa cần thêm mart.
5. **Online ở Q5 tính theo vị trí (`merchant_city = ONLINE`), không theo kênh.** Mart địa lý nhóm theo `dim_location`, và cờ nước ngoài cũng nằm ở đó. Hai cách tính chỉ lệch nhau 5.788 giao dịch Chip có city ONLINE, đúng bằng số vi phạm R07.
6. **Phân quyền theo nhóm.** Rule viết cho nhóm `engineer`, `analyst` thay vì cho từng user. Thêm người hay tài khoản dịch vụ chỉ cần thêm vào `groups.txt`. User không thuộc nhóm nào thì bị chặn theo mặc định.
7. **Dashboard dựng bằng script, không lưu file export.** File export của Superset chứa id, ngày giờ, tham số do giao diện sinh ra, nên khó đọc và khó review. Script ngắn hơn, chart cùng loại dùng chung hàm, và D3, D4 ở tuần 10 chỉ cần thêm khai báo.
8. **Phân bố theo bang dùng treemap, không dùng bản đồ.** Chart `country_map` của Superset 6 chỉ tô màu theo giá trị khi không có bảng màu phân loại (`colorScheme ? tô theo bang : tô theo giá trị`). Trên dashboard, Superset luôn ghi bảng màu mặc định vào dashboard rồi đẩy xuống mọi chart, nên bản đồ tô mỗi bang một màu ngẫu nhiên và không cho thấy bang nào nhiều giao dịch. Treemap cho thấy đủ 52 bang, diện tích tỷ lệ với số giao dịch.
9. **Ghi đè bằng `ImportAssetsCommand`, không dùng `superset import-dashboards`.** Lệnh CLI chỉ ghi đè dashboard; chart và dataset đã có thì giữ bản cũ, nên sửa một chart rồi import lại sẽ không có tác dụng.

## Lưu ý

- **User Trino do client tự khai.** Phân quyền ngăn được dùng nhầm (VD Superset đọc Silver), nhưng không ngăn được ai cố tình khai là `engineer`. Muốn cưỡng chế thật cần bật xác thực mật khẩu hoặc OAuth2 của Trino, mà các cách này bắt buộc HTTPS (README, mục Bảo mật).
- **`make dashboards` cần Gold có dữ liệu,** vì bước lấy cột chạy truy vấn của từng dataset. Trên stack trống, chạy sau khi nạp xong ([tuần 8](week8.md), mục Cách dùng).
- **Chỉ đo thời gian khi tab đang hiển thị.** Tab chạy nền thì Chrome hoãn việc dựng trang: một lần đo khi tab nền cho thấy request dữ liệu bắt đầu ở giây 12 dù mỗi request chỉ mất 1–2,5 giây.
- **Superset không bật cache dữ liệu,** nên mỗi lần mở dashboard đều truy vấn Trino. Thời gian ở trên là thời gian khi không có cache.
- **Không mount từng file vào container.** Bind mount một file gắn theo inode: khi file trên host bị ghi lại thành file mới (nhiều trình soạn thảo, `sed -i`), container vẫn thấy bản cũ. Vì vậy cả `infra/superset` và `sql/queries` được mount theo thư mục.
- `make verify-queries` cần bộ dữ liệu gốc trong `data/raw`; DuckDB nạp 13,3 triệu giao dịch vào bộ nhớ.
- Q7–Q10 đã có truy vấn và đã đối chiếu. Dashboard D3 (Q7–Q9), D4 (Q10) làm ở tuần 10.
