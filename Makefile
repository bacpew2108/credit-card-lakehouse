# Credit Card Lakehouse – lệnh vận hành (mục 10 Design Doc)
SHELL := /bin/bash
COMPOSE := docker compose
DATA_DIR ?= data/raw
PY := uv run --no-project --with-requirements requirements-dev.txt python
SEED := $(PY) generators/seed_source.py --data-dir "$(DATA_DIR)"


.DEFAULT_GOAL := help
.PHONY: help env secrets up up-core down clean ps logs config profile smoke trino seed seed-status source-sql demo erase test

help: ## Liệt kê các lệnh
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

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

demo: ## (Tuần 10) Chạy 8 kịch bản mục 11.1
	@echo "Chưa hiện thực (tuần 10)"; exit 1

erase: ## (Tuần 10) Quyền xóa dữ liệu: make erase CUSTOMER_ID=…
	@echo "Chưa hiện thực (tuần 10)"; exit 1

test: ## Chạy pytest
	$(PY) -m pytest -q tests
