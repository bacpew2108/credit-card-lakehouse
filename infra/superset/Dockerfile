# Superset + driver PostgreSQL (metadata) và Trino (nguồn dữ liệu gold/ops)
FROM apache/superset:6.0.0

USER root
COPY requirements-local.txt /tmp/requirements-local.txt
RUN uv pip install --no-cache --python /app/.venv/bin/python -r /tmp/requirements-local.txt
USER superset
