# Troubleshooting

## Setup

| Symptom | Cause | Fix |
|---|---|---|
| `ERROR (ConfigurationError): Set SOURCE_DB_URL and WAREHOUSE_DB_URL` | No `.env` | `cp .env.example .env` and set both URLs. The databases are created automatically |
| `JAVA_GATEWAY_EXITED` / `Unable to locate a Java Runtime` | PySpark 4 needs Java 17+ | Install a JDK 17 and set `JAVA_HOME` |
| `PYTHON_VERSION_MISMATCH` | Spark workers using a different Python | `get_spark` sets `PYSPARK_PYTHON` to the driver's interpreter. If you set it yourself, point it at the venv's Python |
| `dbt executable not found` | dbt not in the active environment | `make install`; the runner looks next to `sys.executable` first, then on `PATH` |
| `pip install` conflicts when adding Airflow to `.venv` | Airflow pins hundreds of packages | Keep Airflow in `.venv-airflow` (`make install-airflow`); the DAG calls the pipeline venv |

## Running batches

| Symptom | Cause | Fix |
|---|---|---|
| `OutOfOrderBatchError` | Re-running a batch older than the latest succeeded one would roll back the customer/product snapshots | Re-run the latest batch, or rebuild from scratch (`make docker-down` / drop both DBs + `rm -rf lake data/landing`) and replay in order |
| `DataQualityError: … quarantine rate X% exceeds 5%` | Too many rows rejected | Inspect `lake/quarantine/<entity>/batch_id=<date>/` (`_reject_reason` column) and `lake/_metrics/batch_id=<date>/quality.json`. Fix the source, then re-run the same batch |
| `DataQualityError: <entity>.<column>: expect_…` | A critical expectation failed | Same as above; nothing was loaded into the warehouse |
| `ReconciliationError: load_complete:… expected X, got Y` | Warehouse and Silver disagree (e.g. a manual edit in `lake_silver`) | Re-run the latest batch. Loads are upserts, so this repairs it |
| `DbtError: … Failure in test …` | A dbt test failed | Read the failing test name in the error; compiled SQL is under `dbt/target/compiled/`; run `dbt test --select <test> --store-failures` to inspect rows |
| A batch shows `failed` in `meta.ingestion_batches` | Some step raised | Fix the cause and re-run that batch. If you skip it instead, the next batch's window starts at the last *succeeded* batch, so its OLTP changes are not lost |
| Quality report shows 22/23 checks | Intentional. The one non-pass is the **warning**-level check `orders.customer_id ∈ Silver customers`, triggered by the late-arriving customer the generator adds from batch 2 on (`unexpected_count: 1`). Warnings never block a batch | Nothing to fix; see README "Why the quality report says 22/23" |
| Orders with `customer_resolution = 'unknown'` | The customer's CRM record hasn't arrived yet (late-arriving dimension) | Normal for the most recent day; re-attributed automatically by the next batch. The dbt test fails if this persists more than 2 days |
| Leftover `.orders_staging_*` directories | A run crashed mid-merge | Removed automatically by the next merge |

## Airflow

| Symptom | Fix |
|---|---|
| DAG not visible | `AIRFLOW__CORE__DAGS_FOLDER` must point to `airflow/dags`. Check `airflow dags list-import-errors` |
| Task fails with `No such file or directory: /opt/pipeline-venv/bin/python` | Running outside Docker: set `PIPELINE_PYTHON=$PWD/.venv/bin/python` and `LAKEHOUSE_PROJECT_DIR=$PWD` |
| `airflow dags test` complains about migrations | `airflow db migrate` first |
| Tests: `ModuleNotFoundError: airflow.models` | Run DAG tests with `.venv-airflow` (`make test-dag`); in `.venv` they are skipped |

## Docker

| Symptom | Fix |
|---|---|
| `set POSTGRES_PASSWORD in .env` | `cp .env.example .env` and set it |
| The databases weren't created | `docker/postgres-init.sql` only runs on an **empty** volume: `docker compose down -v` then `up` |
| Airflow UI login prompt | `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True` is set for local use; check `docker compose logs airflow` |
| Reset everything | `docker compose down -v` |

## Known dependency advisories

`sqlparse 0.5.4`: PYSEC-2026-3696/3697/3698/3699/3923, fixed in 0.6.0. Every dbt-core 1.10.x requires `sqlparse<0.5.5`, so it cannot be upgraded until dbt does. sqlparse only processes this project's own dbt SQL, never external input. CI ignores exactly these IDs.
