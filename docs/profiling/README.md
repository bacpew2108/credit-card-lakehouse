# Profiling dữ liệu – tổng kết tuần 1

Số liệu chi tiết: [profiling_report.md](profiling_report.md) (sinh lại bằng `make profile`).

## Quy mô đã chốt (thay cho số ước lượng ở mục 3.1 Design Doc)

| File | Quy mô thực tế |
|---|---|
| users_data.csv | 2.000 khách hàng (1.219 khách có giao dịch) |
| cards_data.csv | 6.146 thẻ (4.071 thẻ có giao dịch) |
| transactions_data.csv | 13.305.915 giao dịch, **01/2010 → 10/2019 (118 tháng)**, 1,26 GB |
| mcc_codes.json | 109 mã, đều xuất hiện trong giao dịch |
| train_fraud_labels.json | Không dùng (nhãn học máy – ngoài phạm vi) |

- Một batch tháng có 93–119 nghìn dòng, trung bình khoảng 113 nghìn.
- Năm 2019 chỉ có 10 tháng. Kế hoạch nạp: backfill 108 tháng (2010–2018), sau đó phát hành từng tháng từ 2019-01 đến 2019-10.

## Phát hiện ảnh hưởng đến thiết kế

| # | Phát hiện | Tác động |
|---|---|---|
| 1 | Trường `errors` có nhiều lỗi được bọc `"…"`. 20 nghìn dòng đầu không có trường như vậy nên công cụ tự đoán định dạng CSV sẽ đọc sai | `seed_source.py` phải khai báo rõ `quote='"'` |
| 2 | Kênh Chip chỉ xuất hiện **từ năm 2015** (0% trước 2015, khoảng 71% từ 2015) | Có sẵn câu trả lời rõ ràng cho Q2, nên đưa lên D1 |
| 3 | **5.788** giao dịch `Chip Transaction` có `merchant_city = ONLINE` | R07 có vi phạm thật, minh họa được WARN |
| 4 | Dữ liệu gốc gần như sạch: R01–R06, R08 và R09 đều **0 vi phạm** | **Cần chèn lỗi có kiểm soát** khi seed (VD `make seed MONTH=… INJECT_FAULTS=1`) để demo quarantine, BLOCK và nâng WARN thành BLOCK |
| 5 | Chỉ **9,7%** số thẻ thỏa Luhn (số giả) | **R13 phải đổi**: quét mọi chuỗi 15–16 chữ số hoặc so khớp HMAC với tập token, không lọc theo Luhn |
| 6 | 309 giao dịch xảy ra trước tháng mở thẻ, 83 giao dịch sau tháng hết hạn | Ứng viên rule mới **R14 (WARN)**: giao dịch nằm ngoài thời hạn thẻ |
| 7 | `merchant_state` gồm 52 mã bang Mỹ và **147 tên quốc gia** (89.006 giao dịch nước ngoài). `zip` chỉ null khi giao dịch online hoặc nước ngoài | Cờ `is_foreign = length(state) <> 2`; zip null là hợp lệ |
| 8 | 803.000 zip mất số 0 đầu (VD `2840.0` → `02840`) | `lpad(split_part(zip,'.',1), 5, '0')` |
| 9 | 21,6% merchant có nhiều vị trí (tối đa 4.403); 456 merchant có 2 MCC | silver.merchants lấy vị trí xuất hiện nhiều nhất; MCC giữ ở fact, không đặt ở dim_merchant |
| 10 | Có 7 loại lỗi, 22 tổ hợp lỗi, tối đa 3 lỗi mỗi giao dịch | dim_txn_profile ≤ 3 kênh × 23 tổ hợp (kể cả "không lỗi") × 2 cờ hoàn tiền = 138 dòng |
| 11 | `card_type` có giá trị `Debit (Prepaid)` | Chuẩn hóa về Credit / Debit / Prepaid |
| 12 | 31 thẻ có `credit_limit = 0` | Loại khỏi Q9 để tránh chia cho 0 |
| 13 | `birth_year + current_age = 2019` (năm chụp dữ liệu). Có 1 giao dịch của khách 17 tuổi | Q7 cần nhóm tuổi "< 18" hoặc gộp vào nhóm 18–24 |
| 14 | amount từ −500 đến 6.820,20; 660.049 giao dịch hoàn tiền (4,96%); 10.639 giao dịch amount = 0 | Hoàn tiền không coi là lỗi (đúng như thiết kế) |
| 15 | Thời điểm giao dịch chỉ chính xác đến phút | Đủ cho Q3 (giờ × thứ) |
| 16 | Số giao dịch theo tháng ổn định, không tháng nào lệch quá ±3σ | R12 sẽ không cảnh báo trên dữ liệu gốc, cần chèn lỗi (mục 4) để demo |

## Đề xuất cập nhật Design Doc (v5)

- **Mục 3.1:** thay toàn bộ số ước lượng bằng bảng quy mô ở trên. Ghi rõ dữ liệu kết thúc ở 10/2019.
- **Mục 3.2:**
  - Xác nhận `merchant_state` chứa tên quốc gia.
  - Thêm ghi chú về CSV có trường bọc ngoặc kép.
  - Thêm `card_type = "Debit (Prepaid)"`.
  - Thêm 31 thẻ có `credit_limit = 0`.
- **Mục 3.3:** thêm tùy chọn chèn lỗi khi phát hành batch. "Năm cuối" là 2019 với 10 tháng.
- **Mục 6:**
  - Sửa R13: bỏ điều kiện Luhn.
  - Thêm R14: giao dịch ngoài thời hạn thẻ.
  - Ghi chú rằng dữ liệu gốc sạch nên phần demo DQ dựa vào chèn lỗi.
