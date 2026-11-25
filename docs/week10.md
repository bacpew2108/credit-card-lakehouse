# Tuần 10 – BI (2), tính năng Lakehouse

Demo cuối mốc: 4 dashboard và bộ demo.
- `make dashboards` dựng thêm D3 (12 chart, Q7–Q9) và D4 (10 chart, Q10). Tổng cộng có 17 dataset và 40 chart trên 4 dashboard. Bước kiểm tra chạy truy vấn của cả 40 chart không lỗi, và D3, D4 hiển thị đúng khi mở trên Chrome.
- `make demo` chạy đủ 8 kịch bản của mục 11.1 Design Doc. Cả 8 kịch bản ĐẠT, 28/28 tiêu chí tự kiểm tra (số liệu chi tiết ở mục Cách hoạt động):
  - **ACID:** kill driver Spark sau 47 giây ghi lại fact trong một commit. Trino đọc 53 lần, cả 53 lần đều thấy đúng snapshot cũ và đủ 13.305.915 dòng.
  - **Rollback:** nạp nhầm làm tháng 2019-10 tăng gấp đôi (117.374 → 234.748 dòng); `rollback_to_snapshot` trả về đúng 117.374 dòng.
  - **Bảo trì:** 59 file 72 KiB được gộp thành 2 file, 60 snapshot giảm còn 1.
  - **Tái lập:** `dag_rebuild` cả năm 2018 cho kết quả trùng 12/12 tháng ở cả 5 bảng, so cả số dòng, tổng tiền lẫn checksum mọi cột.
- `make demo S=8 CUSTOMER_ID=1510` xóa khách hàng 1510 (2 thẻ, 1.845 giao dịch trong 44 tháng, 2010-01 → 2013-08) trong khoảng 5 phút:
  - không bảng nào còn dòng của khách;
  - 15/15 bảng không còn đọc được snapshot cũ;
  - 182/182 file từng chứa dữ liệu của khách đã bị xóa khỏi MinIO;
  - fact vẫn đủ 13.305.915 dòng, tổng 571.835.522,28;
  - 1.845 giao dịch nằm ở thẻ Deleted (khóa −2) của `mart_card_monthly`.
- `make verify-queries` vẫn khớp 16/16 truy vấn sau khi xóa: phía DuckDB đọc danh sách xóa và ẩn danh cùng khách hàng. Lần đối chiếu đầu tiên sau khi xóa lệch R14 (392 so với 391). Nguyên nhân là `dq_results` còn giữ kết quả tính trước lúc xóa, trong đó có 1 giao dịch ngoài thời hạn thẻ của khách. Giờ job xóa chạy lại rule trên dữ liệu đã sửa (mục Cách hoạt động). `make roles` đúng 14/14 trường hợp, thêm trường hợp analyst bị chặn đọc danh sách xóa. 135 test qua.

## Sản phẩm

| Sản phẩm | File |
|---|---|
| Dashboard D3 (Q7–Q9), D4 (Q10): dataset, chart, nhãn nhóm theo khoảng giá trị | [infra/superset/dashboards.py](../infra/superset/dashboards.py) |
| Bộ demo 8 kịch bản: chạy, in bằng chứng, tự kiểm tra tiêu chí | [sql/run_demo.py](../sql/run_demo.py) |
| Phần chạy trên Spark của demo: SQL tùy ý, ghi fact trong một commit, đếm file trên MinIO | [spark/demo/iceberg_demo.py](../spark/demo/iceberg_demo.py) |
| Quyền xóa dữ liệu | [spark/maintenance/erase_customer.py](../spark/maintenance/erase_customer.py) |
| Danh sách xóa áp dụng khi trích xuất; khóa −2 trong fact; R04 bỏ qua −2 | [extract_jdbc.py](../spark/extract/extract_jdbc.py), [lakehouse.py](../spark/common/lakehouse.py), [gold_transforms.py](../spark/transform/gold_transforms.py), [rules.py](../spark/quality/rules.py) |
| Q7, Q8 và tính toán độc lập xử lý khách hàng đã xóa | [sql/queries/](../sql/queries), [sql/independent/](../sql/independent), [run_queries.py](../sql/run_queries.py) |
| Kiểm thử (10 test mới, tổng 135) | [tests/test_privacy.py](../tests/test_privacy.py), [test_analytics.py](../tests/test_analytics.py), [test_gold_transforms.py](../tests/test_gold_transforms.py) |
| Lệnh `demo`, `erase` | [Makefile](../Makefile) |

## Cách dùng

```bash
make dashboards                          # D1–D4
make demo                                # kịch bản 1–7, khoảng 13 phút (kịch bản 7 chiếm 10 phút)
make demo S="2 5"                        # chỉ một số kịch bản
make erase CUSTOMER_ID=1510 DRY_RUN=1    # đếm số dòng sẽ bị sửa ở từng bảng, không ghi gì
make demo S=8 CUSTOMER_ID=1510           # XÓA THẬT khách hàng 1510 rồi kiểm tra (không hoàn tác được)
make erase CUSTOMER_ID=1510              # chỉ xóa, không in bằng chứng của kịch bản 8
```

Dashboard mới:
- D3 – Khách hàng và thẻ: http://localhost:8088/superset/dashboard/d3-khach-hang-the/
- D4 – Chất lượng dữ liệu và vận hành pipeline: http://localhost:8088/superset/dashboard/d4-chat-luong-van-hanh/

Trước khi chạy `make demo`, Airflow không được có DAG đang chạy (script tự kiểm tra). Kịch bản 1 dừng rồi khởi động lại container `spark-connect`. Mỗi kịch bản in bằng chứng và các dòng `[ĐẠT]`/`[KHÔNG ĐẠT]`, cuối cùng in bảng tổng kết. Lệnh thoát với mã lỗi nếu có tiêu chí không đạt.

## Cách hoạt động

**Dashboard D3, D4:**

| Dashboard | Chart |
|---|---|
| D3 – Khách hàng và thẻ | 4 số tổng: chi tiêu TB mỗi khách mỗi tháng (4.103 USD), số thẻ có giao dịch, tỷ trọng giá trị của thẻ Credit, tỷ lệ thẻ-tháng vượt hạn mức. Chi tiêu TB theo nhóm tuổi, thu nhập, điểm tín dụng (Q7). Tổng giá trị và số giao dịch theo thương hiệu × loại thẻ, bảng đóng góp từng nhóm (Q8). Phân bố mức sử dụng hạn mức và bảng chi tiết (Q9) |
| D4 – Chất lượng dữ liệu và vận hành pipeline | 4 số tổng: số batch, số dòng nạp, số bản ghi bị cách ly, số batch có rule BLOCK. Số dòng nạp và số rule WARN theo tháng; số vi phạm theo rule kèm bảng kết quả của 13 rule; thời gian chạy theo DAG và kết quả, kèm bảng chi tiết (Q10) |

Kết quả chính:
- **Q7:** chi tiêu tăng mạnh theo thu nhập: nhóm dưới 30k chi 2.677 USD mỗi tháng, nhóm từ 75k trở lên chi 7.382 USD. Theo tuổi, các nhóm từ 18 trở lên chỉ chênh nhau 3.924–4.296 USD.
- **Q8:** thẻ Credit chiếm 39,5% giá trị nhưng chỉ 30,9% số giao dịch.

Nhãn nhóm của mart (`< 18`, `Poor (< 580)`) xếp theo bảng mã sẽ sai thứ tự, nên dataset Q7 có cột `segment_label` đổi sang nhãn bắt đầu bằng khoảng giá trị (`0–17`, `300–579 (Poor)`). Dataset Q9 thì đánh số nhóm (`1. < 10%`).

**Bộ demo** (`sql/run_demo.py` chạy trên host, đọc bằng Trino user `engineer`; bước nào cần Spark thì gọi `spark/demo/iceberg_demo.py` trong `airflow-scheduler`):

| # | Kịch bản | Bằng chứng (lần chạy ngày 25/11) |
|---|---|---|
| 1 | Job Gold ghi lại toàn bộ fact (2010-01 → 2019-10) trong một commit. Khi file mới đã lên MinIO thì `docker kill spark-connect`, trong lúc một luồng Trino truy vấn fact liên tục | Trino đọc 53 lần trong 54 giây, không lỗi, cả 53 lần thấy cùng một snapshot và cùng 13.305.915 dòng, tổng 571.835.522,28. Sau khi kill, snapshot không đổi; 19 file đã ghi dở thành file mồ côi, không snapshot nào tham chiếu |
| 2 | Nạp nhầm tháng 2019-10 bằng `INSERT INTO fact SELECT` (append thay vì ghi đè partition), xem lại bằng `FOR VERSION AS OF`, rồi `rollback_to_snapshot` | Số dòng của tháng: trước khi nạp 117.374; sau khi nạp nhầm 234.748; time travel về snapshot cũ cho 117.374; sau rollback là 117.374 và snapshot hiện tại trở về đúng id cũ |
| 3 | Bản sao `ops.demo.mcc_codes` do Trino tạo: `ADD COLUMN mcc_group`, `RENAME COLUMN description TO mcc_description`, `mcc_code` int → bigint, rồi INSERT một mã 9.999.999.999 | Không file dữ liệu nào bị ghi lại (1 → 2 file, file cũ giữ nguyên). Truy vấn cũ trên snapshot cũ (cột `description`) và truy vấn theo tên mới đều cho (109, 108, 578.917). Spark đọc được bảng do Trino đổi schema: 109 dòng `mcc_group` NULL và dòng mới mang giá trị bigint |
| 4 | Bản sao `ops.demo.fact_partition` (2018-01 → 2019-06) partition theo tháng; đổi sang partition theo năm, INSERT thêm 2019-07 → 2019-10 | `$files`: spec 0 có 18 file theo tháng (2.084.802 dòng), spec 1 có 1 file theo năm (469.956 dòng). Cả 22 tháng trùng fact thật. Lọc 2018-03 chỉ đọc 118.591 dòng (đúng một partition tháng); lọc 2019-08 đọc 469.956 dòng (cả partition năm 2019) |
| 5 | Cùng câu hỏi "tháng 2019-03" trên fact, lọc trực tiếp `transaction_ts` và lọc qua `date_format(transaction_ts)` | Fact không có cột partition. Lọc trực tiếp đọc 118.548 dòng (1,8 MiB, 53 ms); lọc qua hàm đọc cả 13.305.915 dòng (204 MiB, 774 ms); kết quả hai cách như nhau. EXPLAIN in `constraint on [transaction_ts]` |
| 6 | Bản sao `ops.demo.fact_small_files`, 59 INSERT theo ngày (01–02/2019), rồi `rewrite_data_files`, `rewrite_manifests`, `expire_snapshots` | Số file 59 → 2, kích thước TB mỗi file 72 KiB → 1.739 KiB, manifest 59 → 1. Sau compaction, object dữ liệu trên MinIO tăng 59 → 61 vì snapshot cũ còn dùng file cũ. Sau expire: snapshot 62 → 1, object 61 → 2, thư mục bảng 20,7 → 4,0 MiB. Truy vấn quét toàn bảng 314 → 109 ms; 224.501 dòng không đổi |
| 7 | `dag_monthly` tháng 2019-06 với `force` (trích xuất lại từ nguồn); `dag_rebuild` 2018-01 → 2018-12 với `clean` | So số dòng, tổng tiền và checksum mọi cột (hàm `checksum` của Trino, không phụ thuộc thứ tự dòng) của Bronze, Silver, fact, `mart_monthly_kpi`, `mart_card_monthly`: tháng 2019-06 trùng 5/5 bảng (139 giây); năm 2018 trùng 12/12 tháng ở cả 5 bảng (397 giây). Dữ liệu thực sự được ghi lại: Bronze, fact, mart có snapshot mới. Silver không có snapshot mới khi chạy lại một tháng, vì MERGE không ghi gì khi dữ liệu không đổi |
| 8 | `make erase CUSTOMER_ID=1510` rồi thử time travel về snapshot trước lúc xóa | Sửa 1 + 2 + 1.845 dòng ở mỗi tầng, dựng lại mart 44 tháng, R11 PASS. Không còn dòng nào của khách; snapshot cũ của 15 bảng không đọc được; 182/182 file cũ không còn trên MinIO; số dòng và tổng tiền của fact không đổi. Trino `FOR VERSION AS OF` snapshot trước lúc xóa báo `Iceberg snapshot ID does not exists`. `ops.privacy.erasure_log` ghi DONE |

**Quyền xóa dữ liệu** (`erase_customer.py`, chi tiết từng bảng ở [data_model.md](data_model.md) mục 5):
1. **Ghi danh sách xóa trước.** Job ghi khách hàng và danh sách thẻ vào `ops.privacy.erasure_log` (STARTED) trước khi sửa dữ liệu.
2. **Xóa và ẩn danh:**
   - xóa khách hàng, thẻ ở `customers_raw`, `cards_raw`, Silver `customers`, `cards`, `dim_customer`, `dim_card`;
   - giao dịch được giữ lại: id khách và thẻ thành −2 ở Bronze, `quarantine_transactions` và Silver;
   - ở fact: khóa khách và thẻ thành −2, `customer_age_at_txn` thành NULL.

  
3. **Dựng lại mart, chạy lại rule.** Job dựng lại 6 mart của các tháng có giao dịch của khách. Sau đó nó chạy lại R04–R08, R12, R14, R11 cho các tháng đó và R09 cho bảng tham chiếu, để kết quả mới nhất trong `dq_results` khớp dữ liệu đã sửa (Q10, D4).
4. **Xóa lịch sử.** Job chạy `expire_snapshots` để mọi bảng đã sửa chỉ còn snapshot hiện tại, rồi chạy `remove_orphan_files`.
5. **Kiểm tra.** Không bảng nào còn dòng của khách. Đọc snapshot cũ phải lỗi. Các file từng chứa dòng của khách (lấy từ cột metadata `_file` trước khi sửa) phải không còn trên MinIO; việc này kiểm bằng `head_object` với tài khoản chỉ đọc `pan-scanner`, không đi qua catalog. Đạt cả ba thì ghi DONE.

Sau khi xóa, các bước khác xử lý khách hàng này như sau:
- **`extract_jdbc.py`** loại customers, cards của khách ngay trong câu truy vấn nguồn; số dòng nguồn đếm với cùng điều kiện, nên R10 vẫn đúng. Giao dịch của khách được ghi với id `'-2'`.
- **`gold_transforms.fact`** gán khóa −2 cho khóa tự nhiên −2, không gán −1.
- **R04** không coi −2 là lỗi tham chiếu.

Nhờ vậy, chạy lại pipeline từ nguồn cũng không đưa khách hàng trở lại.

## Quyết định thiết kế

1. **Schema evolution, partition evolution, bảo trì làm trên bản sao trong `ops.demo`.** Đổi tên cột hay đổi partition của fact thật sẽ làm hỏng truy vấn Q1–Q10, dashboard, và các cấu hình benchmark C1–C6 của tuần 11. Bản sao được tạo lại ở mỗi lần chạy, nên demo chạy lại được bao nhiêu lần cũng ra cùng kết quả. Kịch bản 1, 2, 7 vẫn dùng bảng thật vì chính chúng chứng minh bảng thật an toàn: kịch bản 1 không commit gì, kịch bản 2 rollback về đúng snapshot cũ, kịch bản 7 ghi lại đúng dữ liệu cũ.
2. **ACID: kill driver, ghi trong một commit.** `gold_fact.py` commit riêng từng tháng, nên dừng giữa chừng thì các tháng đã xong vẫn có snapshot mới. Để kiểm đúng "không có dữ liệu ghi dở", `iceberg_demo.py gold-rewrite` ghi cả 118 tháng (dùng cùng logic `gt.fact`) trong một commit. Script kill container `spark-connect` (SIGKILL), tức kill chính driver đang giữ trạng thái ghi, và chỉ kill khi MinIO đã có file mới. Như vậy lúc dừng chắc chắn đang ghi dở. Sau khi driver chết, client Spark Connect vẫn thử kết nối lại và chưa thoát sau 2 phút, nên script kill luôn tiến trình client.
3. **Xóa khách hàng nhưng giữ giao dịch với khóa −2.** Theo Design Doc, số liệu tổng hợp (số giao dịch, doanh số theo tháng, ngành, địa lý) không đổi sau khi xóa, và R10, R11 vẫn đúng. Ngoài khóa, job còn bỏ tuổi tại thời điểm giao dịch, vì tuổi cùng thời gian giao dịch có thể giúp nhận diện lại khách. Riêng các mart theo khách và thẻ có thay đổi: Q8 có thêm nhóm Deleted, còn Q7 bỏ ô có `customer_count = 0`.
4. **Danh sách xóa nằm trong lakehouse, không sửa hệ thống nguồn.** `core_card` mô phỏng hệ thống thẻ lõi, không thuộc lakehouse. Nếu chỉ xóa ở các tầng thì lần trích xuất sau sẽ nạp khách hàng trở lại. `ops.privacy.erasure_log` chỉ giữ id khách hàng và id thẻ, là lượng thông tin tối thiểu để tôn trọng yêu cầu ở các lần nạp sau. Bảng nằm trong namespace riêng mà vai trò analyst không đọc được.
5. **Expire toàn bộ lịch sử của các bảng bị sửa.** Iceberg không xóa được lịch sử của riêng một khách: mọi snapshot cũ đều trỏ tới file có dòng của khách. Vì vậy job chỉ giữ snapshot hiện tại của các bảng đã sửa, và kiểm tra trực tiếp trên MinIO rằng các file đó đã bị xóa. Cái giá là mất time travel của các bảng này về trước lúc xóa.
6. **Tính toán độc lập áp dụng cùng danh sách xóa.** Nếu không, sau `make erase` thì Q7–Q9 lệch với DuckDB trên file gốc (file gốc vẫn còn khách), và đối chiếu mất ý nghĩa. `run_queries.py verify` đọc danh sách xóa bằng user `engineer`, rồi `_base.sql` ẩn danh giao dịch của các khách này theo đúng quy tắc của `erase_customer.py`.
7. **Demo tự kiểm tra tiêu chí.** Mỗi kịch bản so số liệu và in ĐẠT/KHÔNG ĐẠT, không để người xem tự đọc bảng. Chạy lại trước buổi bảo vệ chỉ cần nhìn bảng tổng kết. Lỗi ở một kịch bản không dừng các kịch bản sau.
8. **So khớp bằng checksum mọi cột, không chỉ số dòng và tổng tiền.** Hai lần dựng có thể trùng số dòng và tổng tiền nhưng khác nội dung, VD gán khóa dimension khác nhau. Hàm `checksum` của Trino không phụ thuộc thứ tự dòng nên so được hai lần ghi khác nhau.

## Lưu ý

- **Kịch bản 8 xóa thật và không hoàn tác được trong lakehouse.** Khách hàng 1510 đã bị xóa ngày 25/11; chạy lại kịch bản 8 với cùng khách chỉ báo "đã xóa trước đó". Muốn đưa khách hàng trở lại thì xóa dòng của khách trong `ops.privacy.erasure_log`, chạy lại `dag_reference` với `force`, rồi `make rebuild FROM=… TO=… CLEAN=1 EXTRACT=1` cho các tháng có giao dịch của khách. Sau khi xóa, D3 có nhóm Deleted ở Q8 và số khách-tháng của Q7 giảm, còn R14 của Q10 còn 391 vi phạm.
- **Tải dashboard sát mục tiêu 5 giây.** Mở trên Chrome, không dùng cache, sau khi Trino đã được làm nóng: D3 mất 4,6–6,6 giây, D4 mất 3,6–5,5 giây. Trong cùng phiên đo, D1 mất 6,1 giây, trong khi tuần 9 đo được 2,3–3,3 giây, nên một phần chênh lệch đến từ trạng thái máy. Có hai nguyên nhân:
  - Chrome chỉ mở 6 kết nối song song tới một host, nên 10–12 chart chạy thành hai lượt.
  - Mỗi mart có 118 file vài KB (partition theo tháng). Mỗi truy vấn phải mở 118 file trên MinIO, và khi 6 truy vấn chạy cùng lúc, mỗi truy vấn mất 1,2–2,5 giây thay vì 0,3–0,5 giây.

  Bố cục mart là việc của tuần 11 (cấu hình C6). Tăng số tiến trình của Superset từ 1 lên 3 không cải thiện nên không giữ.
- **File mồ côi do kịch bản 1 để lại** (19–25 file mỗi lần chạy; sau hai lần chạy là 44 file, 76 MiB) được `dag_maintenance` xóa khi đã cũ hơn 3 ngày. `remove_orphan_files` không cho xóa file mới hơn 24 giờ. Snapshot bị rollback ở kịch bản 2 còn trong metadata cho tới khi `expire_snapshots` xóa (7 ngày).
- **Kịch bản 7 chạy pipeline thật** và mất khoảng 10 phút. `dag_rebuild` với `clean` xóa Silver, fact, mart của năm 2018 trước khi dựng lại, nên trong lúc chạy, các dashboard thiếu dữ liệu năm 2018.
- **Kịch bản 1 dừng `spark-connect`:** job Spark nào đang chạy lúc đó cũng lỗi. Script kiểm tra Airflow trước, nhưng không biết job chạy tay bằng `make`.
