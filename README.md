# Credit Card Lakehouse

Hệ thống Data Lakehouse quy mô nhỏ phục vụ phân tích dữ liệu giao dịch thẻ (Đồ án 2).

Luồng dữ liệu: PostgreSQL `core_card` → Bronze → Silver → Gold (Apache Iceberg trên MinIO) → Trino → Superset, điều phối bằng Airflow. Thiết kế chi tiết nằm trong `Design_Doc.docx`.

## Dữ liệu nằm ở đâu

| Thành phần | Lưu gì |
|---|---|
| PostgreSQL `postgres-source` (`core_card`) | Hệ thống nguồn giả lập: customers, cards, transactions |
| MinIO, bucket `landing` | File thô chưa thành bảng: `reference/mcc_codes/mcc_codes.jsonl` |
| MinIO, bucket `bronze`, `silver`, `gold`, `ops` | Bảng Iceberg của từng tầng, mỗi tầng một bucket: file dữ liệu Parquet (`data/`) và file metadata Iceberg (`metadata/`) |
| Lakekeeper (database `lakekeeper` trong `postgres-meta`) | Mỗi tầng một warehouse cùng tên với bucket: danh sách namespace, bảng và đường dẫn tới file metadata hiện tại của từng bảng |

Mỗi tầng là một catalog trong Spark và Trino (`bronze`, `silver`, `gold`, `ops`), trỏ tới warehouse cùng tên trong Lakekeeper và bucket cùng tên trên MinIO. Bên trong, bảng nghiệp vụ nằm ở namespace `card`, bảng vận hành ở `ops.pipeline`, nên tên đầy đủ có dạng `<tầng>.<namespace>.<bảng>`, ví dụ `silver.card.transactions`. Trong bucket, mỗi bảng là một thư mục theo UUID; muốn biết bảng nằm ở đâu thì xem Lakekeeper UI hoặc truy vấn `SELECT "$path" FROM silver.card.transactions LIMIT 1` qua Trino. Không xóa hay sửa tay file trong các bucket này: catalog sẽ trỏ tới file không còn tồn tại.

Khi Spark hoặc Trino đọc một bảng: hỏi Lakekeeper đường dẫn file metadata hiện tại → đọc metadata trên MinIO để biết file Parquet nào thuộc snapshot mới nhất → đọc các file Parquet đó. Khi ghi: ghi Parquet và metadata mới lên MinIO, rồi Lakekeeper đổi con trỏ sang metadata mới trong một thao tác nguyên tử.

`make storage-status` in hai con số cho từng tầng: dung lượng vật lý của bucket (`mc du`, gồm snapshot cũ, file mồ côi và metadata) và dung lượng dữ liệu đang dùng (tổng `file_size_in_bytes` của snapshot hiện tại). Phần chênh lệch là phần `expire_snapshots` và `remove_orphan_files` sẽ dọn.

## Yêu cầu

- Docker Desktop (Compose v2). Máy nên có ≥ 16 GB RAM, ≥ 4 nhân CPU và ≥ 50 GB đĩa trống. Máy ít RAM thì dùng `make up-core`.
- [uv](https://docs.astral.sh/uv/) để chạy profiling và test trên máy host; Java 17 để `make test` chạy Spark local (lần đầu uv tải pyspark ~430 MB).
- Bộ dữ liệu [Financial Transactions Dataset](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets), giải nén vào `data/raw/` (không commit). Nếu đặt ở chỗ khác thì truyền `DATA_DIR=…`.

## Chạy nhanh

```bash
make env        # tạo .env từ .env.example
make secrets    # sinh khóa, dán vào .env thay cho change_me
make up         # khởi động toàn bộ dịch vụ (make up-core: không có Superset)
make ps         # kiểm tra trạng thái / healthcheck
make smoke      # kiểm chứng Spark ghi → Trino đọc/ghi → Spark đọc cùng bảng Iceberg
make seed       # nạp toàn bộ dữ liệu nguồn vào PostgreSQL core_card (~1 phút; FORCE=1 để nạp lại)
make seed-status
make bronze-ref                              # customers, cards (token hóa), mcc_codes → Bronze
make bronze-range FROM=2010-01 TO=2019-10    # giao dịch 118 tháng → Bronze (make bronze MONTH=… cho một tháng)
make bronze-status
make silver-ref                               # customers, cards, mcc_codes → Silver
make silver-range FROM=2010-01 TO=2019-10     # giao dịch → Silver (MERGE theo tháng, cách ly R01–R03) + merchants
make silver-status
make dq-ref                                   # R09 trên khách hàng, thẻ
make dq-range FROM=2010-01 TO=2019-10         # R10, R01–R03, R04–R08, R12, R14 → ops.pipeline.dq_results (make dq MONTH=… cho một tháng)
make dq-status
make gold-ref                                 # dim_customer, dim_card, dim_mcc, dim_date
make gold-range FROM=2010-01 TO=2019-10       # dim từ giao dịch, fact, 6 mart, đối soát R11 (make gold MONTH=… cho một tháng)
make gold-status
```

Các lệnh trên chạy từng job bằng tay. Để Airflow tự chạy toàn bộ (xem [docs/week8.md](docs/week8.md)):

```bash
make dag-trigger DAG=dag_reference                    # dữ liệu tham chiếu: nguồn → Bronze → Silver → dimension
make dag-runs DAG=dag_reference                       # chờ lần chạy này thành công rồi mới nạp giao dịch
make rebuild FROM=2010-01 TO=2018-12 EXTRACT=1        # dag_rebuild nạp lịch sử 108 tháng trong một lần chạy
make dag-on DAG=dag_monthly                           # catchup 2019-01 → 2019-10, mỗi tháng một lần chạy
make dag-on DAG=dag_maintenance                       # bảo trì Iceberg và quét số thẻ R13 hằng tuần
make runs-status                                      # kết quả từng lần chạy (ops.pipeline.pipeline_runs)
```

| DAG | Lịch | Việc |
|---|---|---|
| `dag_reference` | Thủ công | customers, cards, mcc_codes: nguồn → Bronze → Silver → R09 → dimension tham chiếu |
| `dag_monthly` | Hằng tháng, catchup từ 2019-01 | Một tháng giao dịch từ nguồn đến fact, mart, R11 và báo cáo lần chạy |
| `dag_rebuild` | Thủ công, `start_month`, `end_month` | Dựng lại Silver và Gold từ Bronze cho một khoảng tháng, kiểm chứng kết quả trùng khớp; cũng dùng để nạp lịch sử |
| `dag_maintenance` | Hằng tuần | Gộp file, gộp manifest, xóa snapshot cũ, xóa file mồ côi, quét số thẻ (R13), thống kê kích thước bảng |

Task thất bại thì `on_failure_callback` ghi `ops.pipeline.pipeline_runs`, ghi dòng `[ALERT]` vào log và gửi Telegram nếu `.env` có `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.

Phân tích và dashboard, sau khi Gold có dữ liệu (xem [docs/week9.md](docs/week9.md)):

```bash
make queries                 # 17 truy vấn Q1–Q10 (sql/queries) trên Trino bằng vai trò analyst
make verify-queries          # đối chiếu từng truy vấn với tính toán độc lập bằng DuckDB trên file gốc
make roles                   # kiểm tra phân quyền 2 vai trò engineer, analyst
make dashboards              # tạo/cập nhật dashboard D1, D2 trong Superset (đăng nhập admin / SUPERSET_ADMIN_PASSWORD)
```

Lần đầu `make up` tự build image Spark (tải PySpark ~430 MB), Airflow và Superset, mất vài phút. Những lần sau `make up` dùng lại image đã build, không build lại, và khởi động trong khoảng 35 giây. Sau khi sửa Dockerfile trong `infra/` hoặc đổi phiên bản trong `x-spark-build-args` thì chạy `make build`.

| Dịch vụ | URL |
|---|---|
| MinIO console | http://localhost:9001 |
| Lakekeeper (Iceberg REST Catalog) | http://localhost:8181/ui |
| Trino | http://localhost:8082 (`make trino` để mở CLI) |
| Airflow | http://localhost:8080 |
| Spark master / worker | http://localhost:8090 / http://localhost:8091 |
| Spark UI (driver `spark-connect`) | http://localhost:4040 |
| Superset | http://localhost:8088 |

Gõ `make help` để xem toàn bộ lệnh. Các lệnh `demo`, `erase` sẽ được hiện thực ở tuần 10.

## Phiên bản công nghệ (chốt tuần 1)

| Thành phần | Phiên bản | Trạng thái |
|---|---|---|
| Apache Iceberg | 1.10.1, table format v2 (cùng bản Trino 480 đóng gói) | Kiểm chứng tuần 2 |
| Iceberg REST Catalog | Lakekeeper v0.13.6 | Kiểm chứng tuần 2 |
| Object storage | MinIO `RELEASE.2025-09-07T16-13-09Z` | Đang dùng |
| Trino | 480 (Iceberg connector, native S3) | Đang dùng |
| Apache Spark | PySpark 4.0.4 (Scala 2.13), OpenJDK 17, `iceberg-spark-runtime-4.0_2.13:1.10.1`, `iceberg-aws-bundle:1.10.1`, PostgreSQL JDBC 42.7.8 | Kiểm chứng tuần 2 (`make smoke`) |
| Apache Airflow | 3.3.1, LocalExecutor, image `lakehouse-airflow` có `pyspark-client` 4.0.4; task chạy `python job.py` | Đang dùng |
| Cụm Spark | `spark-master`, `spark-worker` (4 nhân, 2 GB), `spark-connect` (driver thường trực, cổng 15002); image `lakehouse-spark` = Python 3.13 + Java 17 + PySpark và các jar ở trên | Đang dùng |
| Apache Superset | 6.0.0 + `trino` 0.340.0 + `psycopg2-binary` 2.9.13 | Đang dùng |
| PostgreSQL | 16 (`postgres-meta`: 3 database; `postgres-source`: core_card) | Đang dùng |
| Python host | 3.13, DuckDB 1.5.6, psycopg 3.3.5, minio 7.2.20, trino 0.340.0 | Profiling, seed nguồn, đối chiếu truy vấn, test |

## Cấu trúc thư mục

```
├── docker-compose.yml
├── Makefile
├── .env.example          # mẫu biến môi trường (khóa HMAC, mật khẩu)
├── infra/                # airflow, spark, trino, lakekeeper, minio, postgres-meta, postgres-source, superset
│                         # (superset/dashboards.py: dataset, chart, dashboard dựng từ code)
├── data/raw/             # bộ dữ liệu Kaggle (không commit)
├── generators/           # seed_source.py – nạp dữ liệu nguồn vào core_card
├── spark/
│   ├── common/           # helpers.py (hàm thuần), lakehouse.py (session, bảng ops, MERGE)
│   ├── extract/          # extract_jdbc.py, load_files.py
│   ├── transform/        # silver_*.py (+ ref/mcc_groups.csv), gold_*.py, marts.py
│   ├── quality/          # rules.py (rule DQ, dq_results), reconcile.py (R10, R11), pan_scan.py (R13),
│   │                     # run_report.py (pipeline_runs)
│   └── maintenance/      # iceberg_maintenance.py, rebuild.py, storage_report.py; erase_customer.py (tuần 10)
├── airflow/dags/         # dag_reference, dag_monthly, dag_rebuild, dag_maintenance, dag_smoke_test;
│                         # pipeline_tasks.py (task dùng chung), alerts.py (on_failure_callback)
├── sql/                  # queries/ (Q1–Q10 trên Trino), independent/ (cùng câu hỏi bằng DuckDB trên file gốc),
│                         # run_queries.py (chạy, đối chiếu, kiểm tra phân quyền); demo Iceberg (tuần 10)
├── tests/                # pytest; fixtures/ dữ liệu mẫu có lỗi cố ý cho rule DQ
└── docs/
    ├── profiling/        # báo cáo profiling tuần 1
    ├── week2.md … week9.md  # tổng kết từng tuần
    ├── data_model.md     # đặc tả Bronze/Silver/Gold/ops
    ├── source_to_target_mapping.md
    └── data_catalog.csv  # danh mục dữ liệu cá nhân (bản nháp)
```

## Tiến độ

| Tuần | Nội dung | Trạng thái |
|---|---|---|
| 1 | Chốt đề tài, profiling, phiên bản, khung repo | Xong. Xem [docs/profiling/README.md](docs/profiling/README.md) |
| 2 | Dựng môi trường, Spark ghi → Trino đọc | Xong. Xem [docs/week2.md](docs/week2.md) |
| 3 | Nguồn giả lập, thiết kế | Xong. Xem [docs/week3.md](docs/week3.md) |
| 4 | Bronze | Xong. Xem [docs/week4.md](docs/week4.md) |
| 5 | Silver | Xong. Xem [docs/week5.md](docs/week5.md) |
| 6 | Data quality, Airflow (1) | Xong. Xem [docs/week6.md](docs/week6.md) |
| 7 | Gold | Xong. Xem [docs/week7.md](docs/week7.md) |
| 8 | Airflow end-to-end | Xong. Xem [docs/week8.md](docs/week8.md) |
| 9 | SQL phân tích, BI (1) | Xong. Xem [docs/week9.md](docs/week9.md) |
| 10 | BI (2), tính năng | Tiếp theo |

## Bảo mật

### Ai được truy cập bucket nào

Mỗi tài khoản MinIO chỉ có quyền trên đúng bucket nó cần (tạo bởi [infra/minio/init.sh](infra/minio/init.sh)), nên phân quyền được MinIO cưỡng chế, kể cả khi ai đó dùng khóa S3 đọc thẳng file:

| Tài khoản MinIO | Quyền | Ai dùng |
|---|---|---|
| `lakekeeper-bronze`, `-silver`, `-gold`, `-ops` | Đọc/ghi bucket của tầng đó | Credential của warehouse cùng tên trong Lakekeeper |
| `trino` | Đọc `bronze`, `silver`, `gold`; đọc/ghi `ops` | Trino |
| `landing-writer` | Đọc/ghi `landing` | `seed_source.py` |
| `landing-reader` | Đọc `landing` | Job `load_files.py` (boto3) trong Airflow |
| `pan-scanner` | Đọc `bronze`, `silver`, `gold`, `ops` | Job `pan_scan.py` (R13) trong Airflow |

- **Spark không giữ khóa S3.** Lakekeeper ký từng request S3 của Spark (remote signing) bằng credential của warehouse, nên một bảng thuộc warehouse `silver` chỉ chạm được bucket `silver`.
- **Trino: 2 vai trò** (file-based access control, [infra/trino/rules.json](infra/trino/rules.json)). User thuộc nhóm theo [infra/trino/groups.txt](infra/trino/groups.txt): nhóm `engineer` (`trino`, user mặc định của `make trino`; `engineer`) đọc mọi catalog, ghi được `ops`; nhóm `analyst` (`superset`, tài khoản Superset; `analyst`) chỉ đọc `gold.card`, `ops.pipeline`. User không thuộc nhóm nào không truy cập được catalog nào. Catalog `bronze`, `silver`, `gold` còn đặt `iceberg.security=read_only` (ba tầng này do Spark ghi), nên kể cả engineer cũng không ghi được qua Trino. `make roles` thử 13 thao tác đọc/ghi và so với phân quyền mong đợi. Superset không có khóa S3 nên chỉ truy cập dữ liệu qua Trino.
- **Giới hạn:** Lakekeeper và Trino chưa xác thực người dùng (Lakekeeper chạy chế độ allow-all, user Trino do client tự khai, nên ai gọi được cổng 8082 cũng có thể khai là `engineer`; xác thực mật khẩu của Trino bắt buộc HTTPS). Vì vậy mọi job Spark có cùng quyền trên cả bốn warehouse: cô lập giữa các tầng được cưỡng chế ở lưu trữ, nhưng chưa cưỡng chế theo từng job (VD job Silver vẫn có thể ghi vào bảng Bronze qua catalog). Muốn cưỡng chế theo job cần bật xác thực OIDC và phân quyền OpenFGA của Lakekeeper.

### Quét số thẻ (R13)

Số thẻ được token hóa trước khi ghi vào Bronze, nên theo thiết kế không có số thẻ đầy đủ nào trong Lakehouse. R13 kiểm tra lại điều đó trên chính các bucket: `make pan-scan` (task `pan_scan` của `dag_maintenance`) đọc mọi object của `bronze`, `silver`, `gold`, `ops`, gồm cả file của snapshot cũ, manifest và metadata, tìm mọi dãy 13–19 chữ số và so HMAC của từng dãy với các `card_token`. Tìm thấy thì R13 BLOCK, DAG gửi cảnh báo và job Gold dừng cho tới khi một lần quét sau PASS. Chi tiết ở [docs/week8.md](docs/week8.md).

### Dữ liệu cá nhân trong Bronze

Bronze không bật object lock hay versioning của MinIO. Quyền xóa dữ liệu (`erase_customer`) phải xóa được PII của khách hàng (địa chỉ, năm sinh, thu nhập, tọa độ) ở mọi tầng: object lock sẽ chặn việc xóa, còn versioning chỉ thêm delete marker và giữ lại bản cũ, khiến R13 quét bản hiện hành báo sạch trong khi dữ liệu vẫn còn. Số thẻ thì không gặp vấn đề này vì đã được token hóa trước khi ghi vào Bronze. Nếu cần Bronze bất biến cho audit, hướng phù hợp là crypto-shredding: mã hóa PII bằng khóa riêng của từng khách hàng và xóa bằng cách hủy khóa.

### Khóa bí mật

Không bao giờ commit `.env`. `HMAC_SECRET` dùng để token hóa số thẻ, nên không được đổi sau khi đã nạp dữ liệu: đổi khóa đồng nghĩa với việc mọi `card_token` đều thay đổi.
