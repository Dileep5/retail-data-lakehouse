# Verification Report: Retail Data Lakehouse

Date: 2026-11-02. This report lists exactly what was executed, with results. Nothing here is estimated.

## Environment

| Item | Value |
|---|---|
| Machine | Apple M3, 8 GB RAM, macOS (Darwin 27) |
| Python | 3.12.16 (uv-managed); pipeline venv `.venv`, Airflow venv `.venv-airflow` |
| Java | Temurin OpenJDK 17.0.21 (portable, `~/.local/opt/jdk17`) |
| PostgreSQL | 16.15 (portable binaries, local, trust auth). Homebrew refused to run on this macOS version |
| Docker | **not available** |
| Key packages | pyspark 4.0.5, dbt-core 1.10.23, dbt-postgres 1.10.2, great-expectations 1.24.1, pandas 2.3.3, pyarrow 21.0.0, psycopg 3.3.6, apache-airflow 3.3.2 (see `requirements.lock`) |

## Commands executed

| # | Command | Result |
|---|---|---|
| 1 | `uv pip install -r requirements-dev.txt` | PASS |
| 2 | Airflow 3.3.2 install with official constraints into `.venv-airflow` | PASS |
| 3 | `ruff check lakehouse analytics airflow tests` | PASS |
| 4 | `pytest tests/unit -q` | **45 passed** |
| 5 | `TEST_PG_ADMIN_URL=postgresql://postgres@localhost:5432/postgres pytest tests/integration -q` | **8 passed** (2 batches through Spark, GX, PostgreSQL, dbt; idempotent re-run; quality-gate failure path) |
| 6 | `.venv-airflow/bin/python -m pytest tests/airflow -q` | **3 passed** |
| 7 | `airflow db migrate` + `airflow dags test retail_lakehouse 2025-11-04` | PASS: all 7 tasks `success`, DagRun `state=success` |
| 8 | `python -m lakehouse.cli demo --days 3` (fresh databases) | PASS: 3 batches + re-run of 2025-11-03 |
| 9 | Content fingerprint before/after re-running 2025-11-03 | All 9 warehouse tables identical |
| 10 | `dbt test` (via `lakehouse.dbt_runner`) on the demo warehouse | `PASS=38 WARN=0 ERROR=0` |
| 11 | `dbt parse` | PASS |
| 12 | `python -m analytics.report` | PASS: `reports/analytics_report.md` (7 query sections) |
| 13 | YAML parse of `docker-compose.yml`, `.github/workflows/ci.yml`, `dbt_project.yml` | Valid YAML |

## Not executed

| Item | Reason |
|---|---|
| `docker compose up` / `docker build` (both Dockerfiles) | Docker not installed on this machine |
| Airflow scheduler/webserver (`airflow standalone`) | Only `airflow dags test` (in-process execution of every task) was run |
| GitHub Actions workflow | Repository not pushed (awaiting your approval) |

## Bugs found and fixed while testing

1. The generator's injected bad line item reused the next real item ID, so the next batch's first item would have been dropped silently by `ON CONFLICT DO NOTHING`.
2. Spark Python workers used the PATH `python3` (3.10) instead of the driver's interpreter (`PYTHON_VERSION_MISMATCH`). The interpreter is now pinned via `PYSPARK_PYTHON`.
3. Per-entity loads share one transaction, so `ON COMMIT DROP` temp tables collided. Each entity now uses its own temp table.
4. The generator sent orders to the Halifax store before its opening date. Fixed, and a dbt test (`assert_no_orders_before_store_opening`) was added; it failed on the old data with 98 rows.
5. Airflow 3.3 removed `DagBag(include_examples=...)` and `DagBag.get_dag` needs a metadata DB. The tests now handle both Airflow 2 and 3.
6. Great Expectations progress bars and import-time INFO logs flooded task logs. They are now silenced.
