# Báo cáo profiling dữ liệu – Financial Transactions Dataset

Sinh tự động bởi `docs/profiling/profile_dataset.py` lúc 2026-10-28 13:25, DuckDB 1.5.6. Mọi cột được đọc dạng chuỗi (`all_varchar`) như hệ thống nguồn cũ; thời gian nạp 8 giây.

## 1. Quy mô

| File | Số dòng | Dung lượng | Đưa vào |
|---|---|---|---|
| users_data.csv | 2,000 | 0.2 MB | PostgreSQL core_card.customers |
| cards_data.csv | 6,146 | 0.5 MB | PostgreSQL core_card.cards |
| transactions_data.csv | 13,305,915 | 1,258.5 MB | PostgreSQL core_card.transactions (theo tháng) |
| mcc_codes.json | 109 | 0.0 MB | landing/reference/mcc_codes/ (JSON Lines) |
| train_fraud_labels.json | – | 159.1 MB | Không dùng (nhãn gian lận cho học máy – ngoài phạm vi) |

- Khoảng thời gian giao dịch: **2010-01-01 00:01:00 → 2019-10-31 23:59:00**, gồm **118 tháng**.
- Số giao dịch mỗi tháng (= kích thước một batch): nhỏ nhất 93,470, lớn nhất 119,396, trung bình 112,762, trung vị 114,557.

### Số giao dịch và tổng giá trị theo năm

| Năm | Số tháng | Số giao dịch | Tổng amount ($) | Amount TB ($) |
|---|---|---|---|---|
| 2010 | 12 | 1,240,880 | 54,232,556.12 | 43.70 |
| 2011 | 12 | 1,290,770 | 55,778,904.96 | 43.21 |
| 2012 | 12 | 1,321,672 | 56,832,410.86 | 43.00 |
| 2013 | 12 | 1,352,808 | 58,284,939.62 | 43.08 |
| 2014 | 12 | 1,365,537 | 58,617,820.51 | 42.93 |
| 2015 | 12 | 1,388,065 | 59,514,007.43 | 42.88 |
| 2016 | 12 | 1,392,117 | 59,844,028.90 | 42.99 |
| 2017 | 12 | 1,399,308 | 59,628,480.63 | 42.61 |
| 2018 | 12 | 1,394,792 | 59,627,317.94 | 42.75 |
| 2019 | 10 | 1,159,966 | 49,475,055.31 | 42.65 |


### Các tháng của năm cuối (dùng cho phát hành từng tháng)

| Tháng | Số giao dịch | Tổng amount ($) |
|---|---|---|
| 2019-01 | 118,116 | 5,018,161.92 |
| 2019-02 | 106,385 | 4,539,678.49 |
| 2019-03 | 118,548 | 5,077,167.67 |
| 2019-04 | 114,034 | 4,918,536.08 |
| 2019-05 | 117,913 | 5,049,092.44 |
| 2019-06 | 115,014 | 4,960,598.66 |
| 2019-07 | 118,906 | 5,043,365.33 |
| 2019-08 | 118,905 | 5,028,081.01 |
| 2019-09 | 114,771 | 4,850,069.83 |
| 2019-10 | 117,374 | 4,990,303.88 |


## 2. Chất lượng theo cột – transactions

| Cột | Null/rỗng | Tỷ lệ null | Số giá trị khác nhau | Ví dụ |
|---|---|---|---|---|
| id | 0 | 0.00% | 13,305,915 | `7475327` |
| date | 0 | 0.00% | 4,136,496 | `2010-04-13 12:36:00` |
| client_id | 0 | 0.00% | 1,219 | `1166` |
| card_id | 0 | 0.00% | 4,071 | `4168` |
| amount | 0 | 0.00% | 81,161 | `$24.68` |
| use_chip | 0 | 0.00% | 3 | `Swipe Transaction` |
| merchant_id | 0 | 0.00% | 74,831 | `20519` |
| merchant_city | 0 | 0.00% | 12,492 | `Elkhart` |
| merchant_state | 1,563,700 | 11.75% | 199 | `TX` |
| zip | 1,652,706 | 12.42% | 25,256 | `38504.0` |
| mcc | 0 | 0.00% | 109 | `5541` |
| errors | 13,094,522 | 98.41% | 22 | `Insufficient Balance` |


### transaction_id (R01)

- id trùng: **0**; id null: **0**.

### date, amount (R02)

- date không parse được: **0**; amount không parse được: **0**.
- amount dạng `$-77.00`; dòng sai mẫu `$[-]d+.dd`: 0. Khoảng giá trị: -500.00 → 6820.20.
- amount < 0 (hoàn tiền): **660,049** (4.96%); amount = 0: 10,639.

### use_chip – kênh giao dịch (R07)

| Giá trị gốc | Số giao dịch | % |
|---|---|---|
| Swipe Transaction | 6,967,185 | 52.36 |
| Chip Transaction | 4,780,818 | 35.93 |
| Online Transaction | 1,557,912 | 11.71 |

| Kênh | city = ONLINE | city khác ONLINE | state null | zip null |
|---|---|---|---|---|
| Chip Transaction | 5,788 | 4,775,030 | 5,788 | 46,301 |
| Online Transaction | 1,557,912 | 0 | 1,557,912 | 1,557,912 |
| Swipe Transaction | 0 | 6,967,185 | 0 | 48,493 |

- Vi phạm R07 (Online ⇔ merchant_city = ONLINE): **5,788** giao dịch không phải Online nhưng city = ONLINE; 0 giao dịch Online nhưng city khác ONLINE.
- Chuẩn hóa channel: `Swipe Transaction` → Swipe, `Chip Transaction` → Chip, `Online Transaction` → Online.

### Cơ cấu kênh theo năm (Q2)

| Năm | % Swipe | % Chip | % Online |
|---|---|---|---|
| 2010 | 89.10 | 0.00 | 10.90 |
| 2011 | 89.20 | 0.00 | 10.80 |
| 2012 | 88.90 | 0.00 | 11.10 |
| 2013 | 88.40 | 0.00 | 11.60 |
| 2014 | 88.20 | 0.00 | 11.80 |
| 2015 | 16.90 | 70.80 | 12.20 |
| 2016 | 16.80 | 71.00 | 12.20 |
| 2017 | 16.80 | 71.20 | 12.10 |
| 2018 | 16.90 | 71.00 | 12.10 |
| 2019 | 17.00 | 71.00 | 12.10 |


### merchant_state – giao dịch nước ngoài

- Mã bang 2 ký tự: 11,653,209 giao dịch / 52 giá trị. Giá trị khác (tên quốc gia): **89,006 giao dịch / 147 giá trị** → xác nhận merchant_state chứa tên quốc gia với giao dịch nước ngoài.

Top 10 giá trị không phải mã bang:

| merchant_state | Số giao dịch | zip null |
|---|---|---|
| Mexico | 27,401 | 27,401 |
| Canada | 10,647 | 10,647 |
| Italy | 7,081 | 7,081 |
| United Kingdom | 4,482 | 4,482 |
| Germany | 3,025 | 3,025 |
| China | 2,908 | 2,908 |
| France | 2,836 | 2,836 |
| Japan | 2,107 | 2,107 |
| Spain | 1,651 | 1,651 |
| India | 1,608 | 1,608 |


### zip

- zip có giá trị: 11,653,209; dạng số thực `ddddd.0`: **11,653,209**.
- zip null: 1,652,706, trong đó 1,652,706 là giao dịch online hoặc nước ngoài → zip chỉ null khi merchant không ở Mỹ.
- Phần nguyên < 5 chữ số (mất số 0 đầu, cần `lpad(…, 5, '0')`): **803,000**; > 5 chữ số: 0.

| Bang | zip gốc | zip chuẩn hóa | Số giao dịch |
|---|---|---|---|
| RI | 2840 | 02840 | 18,702 |
| NJ | 7922 | 07922 | 17,872 |
| MA | 2151 | 02151 | 14,061 |
| CT | 6854 | 06854 | 11,289 |
| NJ | 7945 | 07945 | 10,559 |


### errors

- Trường errors có nhiều lỗi được bọc trong dấu ngoặc kép (CSV chuẩn RFC 4180); 20 nghìn dòng đầu không có trường nào như vậy nên công cụ tự đoán định dạng dễ đọc sai → seed_source.py phải khai báo rõ quote.
- Giao dịch có lỗi: **211,393** (1.59%); có nhiều lỗi ghép bằng dấu phẩy: **941**; nhiều nhất 3 lỗi/giao dịch.

| Loại lỗi | Số lần xuất hiện |
|---|---|
| Insufficient Balance | 131,621 |
| Bad PIN | 32,482 |
| Technical Glitch | 26,633 |
| Bad Card Number | 7,925 |
| Bad Expiration | 6,295 |
| Bad CVV | 6,241 |
| Bad Zipcode | 1,138 |

Số tổ hợp lỗi khác nhau (cho dim_txn_profile): **22**.

### Giờ trong ngày (Q3)

- Số giao dịch có giây ≠ 0: 0 (thời điểm chỉ chính xác đến phút).

## 3. Toàn vẹn tham chiếu

| Rule | Kiểm tra | Số giao dịch vi phạm |
|---|---|---|
| R04 | client_id không có trong users | 0 |
| R04 | card_id không có trong cards | 0 |
| R05 | Thẻ không thuộc khách hàng (cards.client_id ≠ client_id) | 0 |
| R06 | mcc không có trong mcc_codes | 0 |
| R08 | Thẻ has_chip = NO nhưng có giao dịch chip | 0 |
| – | cards.client_id không có trong users | 0 |

- Số MCC xuất hiện trong giao dịch: 109; số mã trong mcc_codes.json không được dùng: 0.
- Khách hàng không có giao dịch: 781; thẻ không có giao dịch: 2,075.
- Giao dịch trước tháng mở thẻ (acct_open_date): **309**; sau tháng hết hạn (expires): **83** → ứng viên rule WARN bổ sung.

### Tuổi khách tại thời điểm giao dịch (Q7)

| Nhóm tuổi | Số giao dịch | Tuổi min | Tuổi max |
|---|---|---|---|
| < 18 | 1 | 17 | 17 |
| 18–24 | 359,724 | 18 | 24 |
| 25–34 | 2,109,324 | 25 | 34 |
| 35–44 | 3,232,518 | 35 | 44 |
| 45–54 | 3,136,301 | 45 | 54 |
| 55–64 | 2,114,697 | 55 | 64 |
| 65+ | 2,353,350 | 65 | 101 |


## 4. Merchant (suy ra silver.merchants)

- Số merchant: **74,831**.
- Merchant có nhiều vị trí (city, state, zip): **16,182** (21.6%), nhiều nhất 4,403 vị trí → ghi vào DQ, lấy vị trí xuất hiện nhiều nhất.
- Merchant có nhiều MCC: **456**, nhiều nhất 2 MCC → giữ mcc ở fact thay vì ở dim_merchant.

## 5. users_data.csv (customers)

| Dòng | id khác nhau | Tuổi min | Tuổi max | birth_year min | birth_year max |
|---|---|---|---|---|---|
| 2,000 | 2,000 | 18 | 101 | 1,918 | 2,002 |

- birth_year + current_age phổ biến nhất = **2019** → năm chụp dữ liệu; tuổi tại thời điểm giao dịch phải tính từ birth_year.

| gender | Số khách |
|---|---|
| Female | 1,016 |
| Male | 984 |

Các cột tiền (định dạng `$1234`):

| Cột | Ví dụ | Không parse được | Min | Max | = 0 |
|---|---|---|---|---|---|
| per_capita_income | $29278 | 0 | 0.00 | 163,145.00 | 15 |
| yearly_income | $59696 | 0 | 1.00 | 307,018.00 | 0 |
| total_debt | $127613 | 0 | 0.00 | 516,263.00 | 102 |

- credit_score: 480 → 850; ngoài [300, 850] (R09): **0**.
- num_credit_cards khác số thẻ thực tế trong cards (R09): **0** khách; khách không có thẻ nào: 0.
- Giá trị retirement_age, latitude, longitude, address: không null (0 dòng thiếu).

## 6. cards_data.csv (cards)

| card_brand | Số thẻ |
|---|---|
| Mastercard | 3,209 |
| Visa | 2,326 |
| Amex | 402 |
| Discover | 209 |

| card_type | Số thẻ |
|---|---|
| Debit | 3,511 |
| Credit | 2,057 |
| Debit (Prepaid) | 578 |

| has_chip | Số thẻ |
|---|---|
| YES | 5,500 |
| NO | 646 |

| card_on_dark_web | Số thẻ |
|---|---|
| No | 6,146 |

- expires sai dạng MM/YYYY: 0; acct_open_date sai dạng: 0.
- expires: 07/1997 → 12/2024; acct_open_date: 01/1991 → 02/2020; mở sau ngày hết hạn: 0.
- credit_limit ví dụ `$24295`; không parse được: 0; khoảng 0.00 → 151223.00; bằng 0: 31 (cần loại khi tính mức sử dụng hạn mức – Q9).

| card_type | Số thẻ | credit_limit = 0 |
|---|---|---|
| Credit | 2,057 | 26 |
| Debit | 3,511 | 3 |
| Debit (Prepaid) | 578 | 2 |


### Dữ liệu nhạy cảm: card_number, cvv (M5, R13)

- Độ dài card_number: 15 chữ số: 402, 16 chữ số: 5,744.
- Thỏa kiểm tra Luhn: **599/6,146** (9.7%); số thẻ trùng: 0.
  → **Phần lớn số thẻ là số giả, không thỏa Luhn.** Nếu R13 chỉ quét chuỗi 13–19 chữ số + Luhn thì sẽ bỏ sót 5,547 số thẻ bị lọt. R13 cần quét mọi chuỗi 15–16 chữ số (bỏ điều kiện Luhn), hoặc so khớp HMAC của chuỗi tìm được với tập card_token.
- cvv: độ dài 1–3 ký tự; bị loại ngay ở bước trích xuất.
- BIN (6 số đầu) khác nhau: 6,074; last4 khác nhau: 4,577.

## 7. mcc_codes.json

- JSON object duy nhất `{"mcc": "mô tả"}`, 109 mã → cần chuyển sang JSON Lines `{"mcc_code": …, "description": …}` trước khi đặt vào bucket landing.
- Mô tả không có nhóm ngành → cần bảng ánh xạ tự xây mcc_group (≈10 nhóm) cho silver.mcc_codes.

| mcc_code | description |
|---|---|
| 1711 | Heating, Plumbing, Air Conditioning Contractors |
| 3000 | Steelworks |
| 3001 | Steel Products Manufacturing |
| 3005 | Miscellaneous Metal Fabrication |
| 3006 | Miscellaneous Fabricated Metal Products |


## 8. Biến động theo tháng (cơ sở cho R12 ±3σ)

Tháng có số giao dịch hoặc tổng tiền lệch quá ±3σ so với 12 tháng trước (R12 sẽ cảnh báo):

| Tháng | Số giao dịch | z (số giao dịch) | Tổng tiền | z (tổng tiền) |
|---|---|---|---|---|
| 2019-02 | 106,385 | -2.84 | 4,539,678.00 | -3.08 |

Tháng 2 có ít ngày hơn nên số giao dịch và tổng tiền thấp hơn các tháng khác; R12 chỉ cảnh báo (WARN), không chặn. Chia cho số ngày trong tháng không giúp được: số giao dịch tăng dần theo năm nên khi đó lại có nhiều tháng bị báo nhầm hơn.

_Tổng thời gian profiling: 15 giây._
