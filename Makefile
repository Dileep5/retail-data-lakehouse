PY ?= .venv/bin/python
AIRFLOW_PY ?= .venv-airflow/bin/python

.PHONY: install install-airflow lint test test-integration test-dag init demo run report docker-up docker-demo docker-down

install:
	python3.12 -m venv .venv && $(PY) -m pip install -r requirements-dev.txt

install-airflow:
	python3.12 -m venv .venv-airflow && $(AIRFLOW_PY) -m pip install "apache-airflow==3.3.2" pytest \
	  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"

lint:
	$(PY) -m ruff check lakehouse analytics airflow tests

test:
	$(PY) -m pytest tests/unit -q

test-integration:   ## needs TEST_PG_ADMIN_URL, e.g. postgresql://postgres:pw@localhost:5432/postgres
	$(PY) -m pytest tests/integration -q

test-dag:
	$(AIRFLOW_PY) -m pytest tests/airflow -q -c /dev/null --rootdir .

init:
	$(PY) -m lakehouse.cli init

demo:               ## 3 daily batches + an idempotent re-run of the last one
	$(PY) -m lakehouse.cli demo --days 3

run:                ## make run DATE=2025-11-04
	$(PY) -m lakehouse.cli run --batch-date $(DATE)

report:
	$(PY) -m analytics.report

docker-up:
	docker compose up --build -d postgres airflow

docker-demo:
	docker compose --profile cli run --rm pipeline demo --days 3

docker-down:
	docker compose down -v
