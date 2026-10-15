# Credit Card Lakehouse – lệnh vận hành (mục 10 Design Doc)
SHELL := /bin/bash
COMPOSE := docker compose
DATA_DIR ?= data/raw
PY := uv run --no-project --with-requirements requirements-dev.txt python
SEED := $(PY) generators/seed_source.py --data-dir "$(DATA_DIR)"
SPARK := $(COMPOSE) exec -T airflow-scheduler spark-submit /opt/airflow/spark
TRINO := $(COMPOSE) exec -T trino trino --catalog lakehouse --output-format ALIGNED --execute


.DEFAULT_GOAL := help
.PHONY: help env secrets up up-core down clean ps logs config profile smoke trino seed seed-status source-sql bronze-ref bronze bronze-range bronze-status silver-ref silver silver-range silver-merchants silver-status demo erase test

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
	@echo "MINIO_SECRET_KEY=$$(openssl rand -hex 20)"
	@for v in MINIO_ROOT_PASSWORD META_DB_ADMIN_PASSWORD LAKEKEEPER_DB_PASSWORD AIRFLOW_DB_PASSWORD \
	  SUPERSET_DB_PASSWORD SOURCE_DB_PASSWORD AIRFLOW_ADMIN_PASSWORD SUPERSET_ADMIN_PASSWORD; do \
	  echo "$$v=$$(openssl rand -hex 12)"; done

up: env ## Khởi động toàn bộ dịch vụ (core + bi)
	$(COMPOSE) --profile bi up -d --build --wait
	@$(MAKE) --no-print-directory ps

up-core: env ## Khởi động không có Superset (máy ít RAM, khi backfill)
	$(COMPOSE) up -d --build --wait
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
	$(COMPOSE) exec -T airflow-scheduler spark-submit /opt/airflow/spark/smoke_test.py write
	$(COMPOSE) exec -T trino trino --catalog lakehouse --output-format ALIGNED --execute \
	  "SELECT writer, count(*) AS n FROM ops.smoke_test GROUP BY writer; \
	   INSERT INTO ops.smoke_test VALUES (4, 'trino', TIMESTAMP '2019-03-04 10:00:00'); \
	   SELECT * FROM ops.smoke_test ORDER BY id"
	$(COMPOSE) exec -T airflow-scheduler spark-submit /opt/airflow/spark/smoke_test.py check

trino: ## Mở Trino CLI trên catalog lakehouse
	$(COMPOSE) exec trino trino --catalog lakehouse

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
	$(TRINO) "SELECT 'transactions_raw' AS tbl, count(*) AS n_rows, count(DISTINCT _batch_id) AS n_batches FROM bronze.transactions_raw \
	  UNION ALL SELECT 'customers_raw', count(*), count(DISTINCT _batch_id) FROM bronze.customers_raw \
	  UNION ALL SELECT 'cards_raw', count(*), count(DISTINCT _batch_id) FROM bronze.cards_raw \
	  UNION ALL SELECT 'mcc_codes_raw', count(*), count(DISTINCT _batch_id) FROM bronze.mcc_codes_raw; \
	  SELECT source, batch_id, source_count, row_count, status, finished_at FROM ops.ingestion_log \
	  ORDER BY finished_at DESC LIMIT 10"

silver-ref: ## Bronze → Silver cho customers, cards, mcc_codes
	$(SPARK)/transform/silver_reference.py

silver: ## Silver giao dịch một tháng rồi cập nhật merchants: make silver MONTH=2019-03
	@test -n "$(MONTH)" || (echo "Thiếu MONTH, VD: make silver MONTH=2019-03"; exit 1)
	$(SPARK)/transform/silver_transactions.py --month $(MONTH)

silver-range: ## Silver nhiều tháng rồi merchants: make silver-range FROM=2010-01 TO=2019-10
	@test -n "$(FROM)" -a -n "$(TO)" || (echo "Thiếu FROM/TO, VD: make silver-range FROM=2010-01 TO=2019-10"; exit 1)
	$(SPARK)/transform/silver_transactions.py --from $(FROM) --to $(TO)

silver-merchants: ## Tính lại silver.merchants từ silver.transactions
	$(SPARK)/transform/silver_merchants.py

silver-status: ## Số dòng 5 bảng Silver và số bản ghi bị cách ly theo rule
	$(TRINO) "SELECT 'transactions' AS tbl, count(*) AS n_rows FROM silver.transactions \
	  UNION ALL SELECT 'customers', count(*) FROM silver.customers \
	  UNION ALL SELECT 'cards', count(*) FROM silver.cards \
	  UNION ALL SELECT 'mcc_codes', count(*) FROM silver.mcc_codes \
	  UNION ALL SELECT 'merchants', count(*) FROM silver.merchants; \
	  SELECT rule_id, count(*) AS n, count(DISTINCT _batch_id) AS n_batches FROM ops.quarantine_transactions GROUP BY rule_id"

demo: ## (Tuần 10) Chạy 8 kịch bản mục 11.1
	@echo "Chưa hiện thực (tuần 10)"; exit 1

erase: ## (Tuần 10) Quyền xóa dữ liệu: make erase CUSTOMER_ID=…
	@echo "Chưa hiện thực (tuần 10)"; exit 1

test: ## Chạy pytest (có Spark local cho hàm biến đổi Silver; cần Java 17)
	uv run --no-project --with-requirements requirements-dev.txt --with pyspark==4.0.4 python -m pytest -q tests
