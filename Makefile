# Credit Card Lakehouse – lệnh vận hành (mục 10 Design Doc)
SHELL := /bin/bash
COMPOSE := docker compose
DATA_DIR ?= data/raw
UV := uv run --no-project --with-requirements requirements-dev.txt
PY := $(UV) python
SEED := $(PY) generators/seed_source.py --data-dir "$(DATA_DIR)"
SPARK := $(COMPOSE) exec -T airflow-scheduler python /opt/airflow/spark
TRINO := $(COMPOSE) exec -T trino trino --output-format ALIGNED --execute
BLOCK_RATIO_ARG = $(if $(BLOCK_RATIO),--block-ratio $(BLOCK_RATIO))
# Docker Desktop Resource Saver: không còn container nào chạy quá 5 phút thì VM ngủ, lệnh đọc được trả
# từ bộ đệm. Bộ đệm trả 404 cho `image inspect` image tên ngắn (postgres:16-alpine, lakehouse-spark:4.0.4…)
# dù image có sẵn, compose tưởng thiếu nên pull rồi build lại. `docker system df` không qua bộ đệm,
# đánh thức VM trước khi compose kiểm tra image.
WAKE := docker system df >/dev/null


.DEFAULT_GOAL := help
.PHONY: help env secrets build up up-core down clean ps logs config profile smoke trino seed seed-status source-sql bronze-ref bronze bronze-range bronze-status silver-ref silver silver-range silver-merchants silver-status dq dq-range dq-ref dq-status gold-ref gold gold-range gold-status storage-status demo erase test

help: ## Liệt kê các lệnh
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  make %-17s %s\n", $$1, $$2}'

env: ## Tạo .env từ .env.example nếu chưa có
	@test -f .env && echo ".env đã tồn tại, giữ nguyên" || (cp .env.example .env && echo "Đã tạo .env – hãy thay các giá trị change_me (make secrets)")

secrets: ## In các khóa ngẫu nhiên để dán vào .env
	@echo "AIRFLOW_FERNET_KEY=$$(openssl rand -base64 32 | tr '+/' '-_')"
	@echo "AIRFLOW_SECRET_KEY=$$(openssl rand -hex 12)"
	@echo "AIRFLOW_JWT_SECRET=$$(openssl rand -hex 12)"
	@echo "SUPERSET_SECRET_KEY=$$(openssl rand -base64 42)"
	@echo "HMAC_SECRET=$$(openssl rand -hex 32)"
	@echo "LAKEKEEPER_ENCRYPTION_KEY=$$(openssl rand -hex 32)"
	@for v in LAKEKEEPER_BRONZE LAKEKEEPER_SILVER LAKEKEEPER_GOLD LAKEKEEPER_OPS TRINO LANDING_WRITER LANDING_READER; do \
	  echo "MINIO_SECRET_$$v=$$(openssl rand -hex 20)"; done
	@for v in MINIO_ROOT_PASSWORD META_DB_ADMIN_PASSWORD LAKEKEEPER_DB_PASSWORD AIRFLOW_DB_PASSWORD \
	  SUPERSET_DB_PASSWORD SOURCE_DB_PASSWORD AIRFLOW_ADMIN_PASSWORD SUPERSET_ADMIN_PASSWORD; do \
	  echo "$$v=$$(openssl rand -hex 12)"; done

# up/up-core không build lại: dùng image đã có, image nào chưa có (máy sạch) thì compose tự build.
# Sửa Dockerfile hoặc đổi phiên bản trong x-spark-build-args thì chạy make build.
build: ## Build lại image Spark, Airflow, Superset (sau khi sửa Dockerfile hoặc đổi phiên bản)
	$(COMPOSE) --profile bi build

up: env ## Khởi động toàn bộ dịch vụ (core + bi)
	@$(WAKE)
	$(COMPOSE) --profile bi up -d --wait
	@$(MAKE) --no-print-directory ps

up-core: env ## Khởi động không có Superset (máy ít RAM, khi backfill)
	@$(WAKE)
	$(COMPOSE) up -d --wait
	@$(MAKE) --no-print-directory ps

down: ## Dừng dịch vụ, giữ dữ liệu
	$(COMPOSE) --profile bi down

clean: ## Dừng và XÓA toàn bộ volume dữ liệu (hỏi xác nhận)
	@read -p "Xóa mọi volume (MinIO, catalog, Airflow, Superset)? [y/N] " a && [ "$$a" = y ]
	$(COMPOSE) --profile bi down -v

ps: ## Trạng thái và healthcheck các container
	$(COMPOSE) --profile bi ps -a --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}'

logs: ## Xem log (make logs S=trino)
	$(COMPOSE) logs -f --tail=200 $(S)

config: ## Kiểm tra cú pháp docker-compose.yml
	$(COMPOSE) config --quiet && echo "docker-compose.yml hợp lệ"

profile: ## Profiling dữ liệu gốc → docs/profiling/profiling_report.md
	$(PY) docs/profiling/profile_dataset.py --data-dir "$(DATA_DIR)" --out docs/profiling/profiling_report.md

smoke: ## Kiểm chứng Spark ghi → Trino đọc/ghi → Spark đọc trên cùng bảng Iceberg
	$(SPARK)/smoke_test.py write
	$(TRINO) \
	  "SELECT writer, count(*) AS n FROM ops.pipeline.smoke_test GROUP BY writer; \
	   INSERT INTO ops.pipeline.smoke_test VALUES (4, 'trino', TIMESTAMP '2019-03-04 10:00:00'); \
	   SELECT * FROM ops.pipeline.smoke_test ORDER BY id"
	$(SPARK)/smoke_test.py check

trino: ## Mở Trino CLI (catalog bronze, silver, gold, ops)
	$(COMPOSE) exec trino trino

seed: ## Nạp toàn bộ dữ liệu nguồn vào core_card (FORCE=1 để nạp lại)
	$(SEED) load $(if $(FORCE),--force)

seed-status: ## Số dòng các bảng core_card, khoảng thời gian giao dịch
	$(SEED) status

source-sql: ## Mở psql vào core_card (make source-sql Q="SELECT …" để chạy một câu)
	$(COMPOSE) exec $(if $(Q),-T) postgres-source \
	  sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" "$$@"' -- $(if $(Q),-c "$(Q)")

bronze-ref: ## Nạp customers, cards (token hóa), mcc_codes vào Bronze (FORCE=1 để nạp lại)
	$(SPARK)/extract/extract_jdbc.py reference $(if $(FORCE),--force)
	$(SPARK)/extract/load_files.py mcc $(if $(FORCE),--force)

bronze: ## Trích xuất giao dịch một tháng vào Bronze: make bronze MONTH=2019-03 [FORCE=1]
	@test -n "$(MONTH)" || (echo "Thiếu MONTH, VD: make bronze MONTH=2019-03"; exit 1)
	$(SPARK)/extract/extract_jdbc.py transactions --month $(MONTH) $(if $(FORCE),--force)

bronze-range: ## Trích xuất nhiều tháng: make bronze-range FROM=2010-01 TO=2019-10 [FORCE=1]
	@test -n "$(FROM)" -a -n "$(TO)" || (echo "Thiếu FROM/TO, VD: make bronze-range FROM=2010-01 TO=2019-10"; exit 1)
	$(SPARK)/extract/extract_jdbc.py transactions --from $(FROM) --to $(TO) $(if $(FORCE),--force)

bronze-status: ## Số dòng các bảng Bronze và 10 lần nạp gần nhất
	$(TRINO) "SELECT 'transactions_raw' AS tbl, count(*) AS n_rows, count(DISTINCT _batch_id) AS n_batches FROM bronze.card.transactions_raw \
	  UNION ALL SELECT 'customers_raw', count(*), count(DISTINCT _batch_id) FROM bronze.card.customers_raw \
	  UNION ALL SELECT 'cards_raw', count(*), count(DISTINCT _batch_id) FROM bronze.card.cards_raw \
	  UNION ALL SELECT 'mcc_codes_raw', count(*), count(DISTINCT _batch_id) FROM bronze.card.mcc_codes_raw; \
	  SELECT source, batch_id, source_count, row_count, status, finished_at FROM ops.pipeline.ingestion_log \
	  ORDER BY finished_at DESC LIMIT 10"

silver-ref: ## Bronze → Silver cho customers, cards, mcc_codes
	$(SPARK)/transform/silver_reference.py

silver: ## Silver giao dịch một tháng rồi cập nhật merchants: make silver MONTH=2019-03 [BLOCK_RATIO=0.01]
	@test -n "$(MONTH)" || (echo "Thiếu MONTH, VD: make silver MONTH=2019-03"; exit 1)
	$(SPARK)/transform/silver_transactions.py --month $(MONTH) $(BLOCK_RATIO_ARG)

silver-range: ## Silver nhiều tháng rồi merchants: make silver-range FROM=2010-01 TO=2019-10
	@test -n "$(FROM)" -a -n "$(TO)" || (echo "Thiếu FROM/TO, VD: make silver-range FROM=2010-01 TO=2019-10"; exit 1)
	$(SPARK)/transform/silver_transactions.py --from $(FROM) --to $(TO) $(BLOCK_RATIO_ARG)

silver-merchants: ## Tính lại silver.card.merchants từ silver.card.transactions
	$(SPARK)/transform/silver_merchants.py

silver-status: ## Số dòng 5 bảng Silver và số bản ghi bị cách ly theo rule
	$(TRINO) "SELECT 'transactions' AS tbl, count(*) AS n_rows FROM silver.card.transactions \
	  UNION ALL SELECT 'customers', count(*) FROM silver.card.customers \
	  UNION ALL SELECT 'cards', count(*) FROM silver.card.cards \
	  UNION ALL SELECT 'mcc_codes', count(*) FROM silver.card.mcc_codes \
	  UNION ALL SELECT 'merchants', count(*) FROM silver.card.merchants; \
	  SELECT rule_id, count(*) AS n, count(DISTINCT _batch_id) AS n_batches FROM ops.pipeline.quarantine_transactions GROUP BY rule_id"

dq: ## Rule DQ một tháng (R10, R01–R03, R04–R08, R12, R14), dừng nếu BLOCK: make dq MONTH=2019-03 [BLOCK_RATIO=0.01]
	@test -n "$(MONTH)" || (echo "Thiếu MONTH, VD: make dq MONTH=2019-03"; exit 1)
	$(SPARK)/quality/reconcile.py src-bronze --month $(MONTH)
	$(SPARK)/quality/reconcile.py bronze-silver --month $(MONTH) $(BLOCK_RATIO_ARG)
	$(SPARK)/quality/rules.py transactions --month $(MONTH) $(BLOCK_RATIO_ARG)

dq-range: ## Rule DQ nhiều tháng trong một session: make dq-range FROM=2010-01 TO=2019-10
	@test -n "$(FROM)" -a -n "$(TO)" || (echo "Thiếu FROM/TO, VD: make dq-range FROM=2010-01 TO=2019-10"; exit 1)
	$(SPARK)/quality/reconcile.py src-bronze --from $(FROM) --to $(TO)
	$(SPARK)/quality/reconcile.py bronze-silver --from $(FROM) --to $(TO) $(BLOCK_RATIO_ARG)
	$(SPARK)/quality/rules.py transactions --from $(FROM) --to $(TO) $(BLOCK_RATIO_ARG)

dq-ref: ## Rule DQ trên bảng tham chiếu (R09)
	$(SPARK)/quality/rules.py reference $(BLOCK_RATIO_ARG)

dq-status: ## Tổng hợp kết quả mới nhất của từng rule và các batch có WARN/BLOCK gần nhất
	$(TRINO) "WITH latest AS (SELECT batch_id, rule_id, max_by(severity, created_at) AS severity, \
	    max_by(status, created_at) AS status, max_by(failed_count, created_at) AS failed_count, \
	    max(created_at) AS created_at FROM ops.pipeline.dq_results GROUP BY batch_id, rule_id) \
	  SELECT rule_id, severity, count(*) AS batches, \
	    if(severity = 'INFO', max_by(failed_count, created_at), sum(failed_count)) AS failed, \
	    count_if(status = 'WARN') AS warn, count_if(status = 'BLOCK') AS block FROM latest \
	    GROUP BY rule_id, severity ORDER BY rule_id; \
	  WITH latest AS (SELECT batch_id, rule_id, max_by(status, created_at) AS status, \
	    max_by(failed_count, created_at) AS failed_count, max_by(checked_count, created_at) AS checked_count, \
	    max_by(details, created_at) AS details FROM ops.pipeline.dq_results GROUP BY batch_id, rule_id) \
	  SELECT batch_id, rule_id, status, failed_count, checked_count, details FROM latest \
	    WHERE status IN ('WARN', 'BLOCK') ORDER BY batch_id DESC, rule_id LIMIT 15"

gold-ref: ## Gold: dim_customer, dim_card, dim_mcc, dim_date (sau silver-ref)
	$(SPARK)/transform/gold_dimensions.py reference

gold: ## Gold một tháng: dim từ giao dịch, fact, 6 mart, R11: make gold MONTH=2019-03
	@test -n "$(MONTH)" || (echo "Thiếu MONTH, VD: make gold MONTH=2019-03"; exit 1)
	$(SPARK)/transform/gold_fact.py --month $(MONTH)
	$(SPARK)/transform/marts.py --month $(MONTH)
	$(SPARK)/quality/reconcile.py silver-gold --month $(MONTH)

gold-range: ## Gold nhiều tháng, mỗi bước một Spark session: make gold-range FROM=2010-01 TO=2019-10
	@test -n "$(FROM)" -a -n "$(TO)" || (echo "Thiếu FROM/TO, VD: make gold-range FROM=2010-01 TO=2019-10"; exit 1)
	$(SPARK)/transform/gold_fact.py --from $(FROM) --to $(TO)
	$(SPARK)/transform/marts.py --from $(FROM) --to $(TO)
	$(SPARK)/quality/reconcile.py silver-gold --from $(FROM) --to $(TO)

gold-status: ## Số dòng các bảng Gold, số khóa −1 trong fact, kết quả R11 mới nhất theo tháng
	$(TRINO) "SELECT 'dim_customer' AS tbl, count(*) AS n_rows FROM gold.card.dim_customer \
	  UNION ALL SELECT 'dim_card', count(*) FROM gold.card.dim_card \
	  UNION ALL SELECT 'dim_merchant', count(*) FROM gold.card.dim_merchant \
	  UNION ALL SELECT 'dim_location', count(*) FROM gold.card.dim_location \
	  UNION ALL SELECT 'dim_mcc', count(*) FROM gold.card.dim_mcc \
	  UNION ALL SELECT 'dim_date', count(*) FROM gold.card.dim_date \
	  UNION ALL SELECT 'dim_txn_profile', count(*) FROM gold.card.dim_txn_profile \
	  UNION ALL SELECT 'fact_transaction', count(*) FROM gold.card.fact_transaction \
	  UNION ALL SELECT 'mart_monthly_kpi', count(*) FROM gold.card.mart_monthly_kpi \
	  UNION ALL SELECT 'mart_mcc_monthly', count(*) FROM gold.card.mart_mcc_monthly \
	  UNION ALL SELECT 'mart_geo_monthly', count(*) FROM gold.card.mart_geo_monthly \
	  UNION ALL SELECT 'mart_error_monthly', count(*) FROM gold.card.mart_error_monthly \
	  UNION ALL SELECT 'mart_customer_segment_monthly', count(*) FROM gold.card.mart_customer_segment_monthly \
	  UNION ALL SELECT 'mart_card_monthly', count(*) FROM gold.card.mart_card_monthly; \
	  SELECT count(*) AS fact_rows, count_if(customer_key < 0) AS customer_unknown, count_if(card_key < 0) AS card_unknown, \
	    count_if(merchant_key < 0) AS merchant_unknown, count_if(location_key < 0) AS location_unknown, \
	    count_if(mcc_key < 0) AS mcc_unknown, count_if(txn_profile_key < 0) AS profile_unknown, \
	    count_if(date_key < 0) AS date_unknown FROM gold.card.fact_transaction; \
	  WITH latest AS (SELECT batch_id, max_by(status, created_at) AS status FROM ops.pipeline.dq_results \
	    WHERE rule_id = 'R11' GROUP BY batch_id) SELECT status AS r11_status, count(*) AS months FROM latest GROUP BY status"

storage-status: ## Dung lượng từng tầng: vật lý (mc du) và dữ liệu đang dùng (snapshot hiện tại)
	@echo "Dung lượng vật lý của bucket (gồm snapshot cũ, file mồ côi, metadata):"
	@$(COMPOSE) exec -T minio sh -c 'mc alias set l http://localhost:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" >/dev/null; \
	  for b in bronze silver gold ops; do mc du l/$$b; done'
	@echo "Dữ liệu đang dùng (file của snapshot hiện tại):"
	@$(SPARK)/maintenance/storage_report.py

demo: ## (Tuần 10) Chạy 8 kịch bản mục 11.1
	@echo "Chưa hiện thực (tuần 10)"; exit 1

erase: ## (Tuần 10) Quyền xóa dữ liệu: make erase CUSTOMER_ID=…
	@echo "Chưa hiện thực (tuần 10)"; exit 1

test: ## Chạy pytest (có Spark local cho hàm biến đổi Silver; cần Java 17)
	$(UV) --with pyspark==4.0.4 python -m pytest -q
