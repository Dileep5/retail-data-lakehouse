# Test Results

Every entry below was executed. Timestamps are UTC.

## Environment

| Item | Value |
|---|---|
| Machine | Apple M3, 8 GB RAM, macOS (Darwin 27) |
| Python | 3.12.16 (`.venv` pipeline, `.venv-airflow` Airflow) |
| Java | Temurin OpenJDK 17.0.21 |
| PostgreSQL | 16.15 (portable binaries, localhost:5432, trust auth) |
| Docker | **not installed**, so no container was built or run |
| Key packages | pyspark 4.0.5, dbt-core 1.10.23, dbt-postgres 1.10.2, great-expectations 1.24.1, pandas 2.3.3, pyarrow 23.0.1, psycopg 3.3.6, apache-airflow 3.3.2, pytest 9.2.0 (`requirements.lock`, `requirements-airflow.lock`) |

## 1. Final test pass (commit `b4328ce`)

```
[2026-11-03T00:10:04Z] $ ruff check lakehouse analytics airflow tests
All checks passed!
[2026-11-03T00:10:04Z] $ pytest tests/unit -q
45 passed in 17.09s
[2026-11-03T00:10:22Z] $ TEST_PG_ADMIN_URL=postgresql://postgres@localhost:5432/postgres pytest tests/integration -q
11 passed in 49.56s
[2026-11-03T00:11:12Z] $ .venv-airflow/bin/python -m pytest tests/airflow -q -c /dev/null --rootdir .
3 passed in 0.96s
[2026-11-03T00:11:13Z] $ uv pip check
All installed packages are compatible
[2026-11-03T00:11:13Z] $ dbt parse --project-dir dbt --profiles-dir dbt
OK
```

| Suite | Passed | Failed | Skipped |
|---|---|---|---|
| Unit: PySpark transforms, merges, GX, generator, windows | 45 | 0 | 0 |
| End-to-end: PostgreSQL + Spark + GX + dbt, 3 batches | 11 | 0 | 0 |
| Airflow DAG integrity (Airflow 3.3.2) | 3 | 0 | 0 |
| **Total pytest** | **59** | **0** | **0** |
| dbt tests (warehouse after the demo run) | 40 | 0 | 0 |

**Baseline before this audit** (2026-11-02 ~23:40Z, commit `7fb07e4`): 53 + 3 = 56 passed, 0 failed; 38 dbt tests.

The audit added:
- 3 end-to-end tests: late line item, late customer re-attribution, out-of-order refusal;
- an extended recovery test;
- 2 dbt tests.

## 1b. Reproduction check (commit `a2a25ce`, 2026-11-03)

The **59 pytest tests** run in two environments: 56 in `.venv` (`pytest` uses `testpaths = tests/unit, tests/integration`) and 3 DAG tests in `.venv-airflow`. Airflow's pinned dependencies are kept out of the pipeline venv, which is why there are two.

```
[2026-11-03T00:39:11Z] $ TEST_PG_ADMIN_URL=postgresql://postgres@localhost:5432/postgres .venv/bin/python -m pytest -q
56 passed in 65.77s
[2026-11-03T00:40:17Z] $ .venv-airflow/bin/python -m pytest tests/airflow -q -c /dev/null --rootdir .
3 passed in 0.97s
[2026-11-03T00:40:19Z] $ python -m lakehouse.cli run --batch-date 2025-11-04      # re-run of the latest batch
7 steps done (silver 9.8s, quality 4.1s: 23 checks / 1 warning, dbt 11.6s, reconcile 14 checks passed)
rerun 2025-11-04: identical tables 9 / 9                                           # fingerprint before vs after
[2026-11-03T00:40:47Z] $ python -m lakehouse.cli run --batch-date 2025-11-02
ERROR (OutOfOrderBatchError): Batch 2025-11-02 is older than succeeded batch 2025-11-04; ...
[2026-11-03T00:40:49Z] $ airflow db migrate && airflow dags test retail_lakehouse 2025-11-05
exit=0  tasks_success=7  DagRun Finished: logical_date=2025-11-05 ... state=success
[2026-11-03T00:41:13Z] $ dbt test   (via lakehouse.dbt_runner, warehouse after batch 2025-11-05)
Done. PASS=40 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=40
quality 2025-11-04: 22/23, non-pass [('orders', 'customer_id', 'warning', 1)], reconcile 14/14
quality 2025-11-05: 22/23, non-pass [('orders', 'customer_id', 'warning', 1)], reconcile 14/14
```

The DEMO.md commands were then executed verbatim from `make reset`:
- 3 batches plus a re-run succeeded, and the fingerprint showed 9/9 tables identical;
- the out-of-order re-run was refused;
- customer 1's SCD2 history and the `[('orders','customer_id','warning',1)]` quality output matched the documented values.

### Why data quality reports 22/23 from batch 2 onward

This is intentional, not a failure. The generator adds one late-arriving customer per batch from batch 2: the order arrives before the customer's CRM row. The expectation `orders.customer_id ∈ Silver customers` has severity **warning** (`lakehouse/quality.py:suites`), so it reports `unexpected_count: 1` and does not block. Only critical expectations, or a quarantine rate above 5%, fail a batch. dbt handles the order with an unknown member and re-attributes it the next day (verified: customer 308 moved from `unknown` to `point_in_time` after batch 2025-11-04).

## 2. Full pipeline run on the sample data (2026-11-02T23:54:16Z → 23:55:26Z)

Fresh databases and an empty lake; `lakehouse.cli run` for each batch.

| Batch | generate | bronze | silver | quality | load | dbt | reconcile |
|---|---|---|---|---|---|---|---|
| 2025-11-01 | 0.1 s | 0.1 s | 8.2 s | 1.7 s | 0.1 s | 5.6 s | 0.0 s |
| 2025-11-02 | 0.0 s | 0.0 s | 8.5 s | 1.8 s | 0.0 s | 5.3 s | 0.0 s |
| 2025-11-03 | 0.0 s | 0.0 s | 8.7 s | 1.7 s | 0.0 s | 5.2 s | 0.0 s |

Silver accounting (bronze rows → quarantined / duplicates / written):

| Batch | customers | products | inventory | orders | order_items | GX | Reconciliation |
|---|---|---|---|---|---|---|---|
| 2025-11-01 | 304 → 2 / 2 / 300 | 41 → 1 / 0 / 40 | 321 → 1 / 0 / 320 | 1158 → 0 / 0 / 1158 | 2843 → 1 / 0 / 2842 | 23/23 | 14/14 |
| 2025-11-02 | 307 → 2 / 2 / 303 | 41 → 1 / 0 / 40 | 321 → 1 / 0 / 320 | 21 → 0 / 0 / 21 | 48 → 1 / 0 / 47 | 22/23 ¹ | 14/14 |
| 2025-11-03 | 311 → 2 / 2 / 307 | 41 → 1 / 0 / 40 | 321 → 1 / 0 / 320 | 28 → 0 / 0 / 28 | 52 → 1 / 0 / 51 | 22/23 ¹ | 14/14 |

¹ Intentional: the non-pass is the *warning-level* check `orders.customer_id ∈ silver customers`: the generated late-arriving customer. Warnings do not block, by design.

Orders extracted for 2025-11-02 include 4 late status changes on earlier orders and one order by the late-arriving customer. Order items include one late item attached to an order placed 2–9 days earlier.

### Idempotent re-run of 2025-11-03

Content fingerprints (row count + MD5 of all rows, `lakehouse/fingerprint.py`), before vs after re-running:

| Table | Rows | Identical |
|---|---|---|
| lake_silver.customers | 307 | yes |
| lake_silver.orders | 1,199 | yes |
| lake_silver.order_items | 2,940 | yes |
| lake_silver.inventory | 960 | yes |
| analytics_snapshots.customers_snapshot | 331 | yes |
| analytics_marts.dim_customers | 332 (331 + unknown member) | yes |
| analytics_marts.fct_orders | 1,199 | yes |
| analytics_marts.fct_order_items | 2,940 | yes |
| analytics_marts.fct_inventory_daily | 960 | yes |

The out-of-order re-run of 2025-11-01 was refused:

```
ERROR (OutOfOrderBatchError): Batch 2025-11-01 is older than succeeded batch 2025-11-03; re-running it would roll back snapshot tables. ...
```

That refusal exposed a bug: the refused request marked the old batch `failed`. It is fixed in `5e04673` and covered by the e2e test.

### Airflow execution (batch 2025-11-04)

`airflow db migrate` + `airflow dags test retail_lakehouse 2025-11-04` (Airflow 3.3.2, SQLite metadata DB, BashOperator → pipeline venv):
- **Result:** all 7 tasks reached `new_state=success`, and the DagRun finished with `state=success`.
- **Late-arrival follow-up:** after this run, 2025-11-03's late customer (308) was re-attributed from `unknown` to `point_in_time`. The only `unknown` left was 2025-11-04's own late customer (314).

## 3. Fresh-clone reproduction (2026-11-03, `git clone` into an empty directory)

| README command | Result |
|---|---|
| `make install` | exit 0 |
| `make install-airflow` | exit 0 |
| `cp .env.example .env` + set URLs to new DBs `fresh_source`/`fresh_dw` | — |
| `make demo` | 3 batches succeeded + idempotent re-run succeeded |
| `make report` | `reports/analytics_report.md` written |
| `make lint` / `make test` | clean / 45 passed |
| `make test-integration` | 11 passed |
| `make test-dag` | 3 passed |

## 4. Dependency and security checks

| Check | Result |
|---|---|
| `pip check` (both venvs) | compatible |
| `pip-audit` | pyarrow 21.0.0 → upgraded to 23.0.1 (PYSEC-2026-113); pytest → 9.2.0. **sqlparse 0.5.4** has 5 advisories fixed in 0.6.0, but every dbt-core 1.10.x pins `sqlparse<0.5.5`; accepted (it only parses this project's own SQL). CI ignores exactly those IDs |
| Linux resolution (`uv pip compile`, x86_64 + aarch64) | resolves (77 pins) |
| Docker Hub tags | `python:3.12-slim-bookworm`, `apache/airflow:3.3.2-python3.12`, `postgres:16-alpine` exist |
| Hardcoded credentials | removed (no default DB URLs); `.env` gitignored |

## Bugs found and fixed during this audit

1. Late-arriving customers made `fct_orders.customer_sk` NULL, so the `not_null` test **halted the pipeline** (reproduced on batch 2025-11-02).
2. Late line items left `fct_orders` revenue stale, because the incremental filter only looked at the order's own batch.
3. A refused out-of-order re-run marked the historical batch `failed`.
4. Default database URLs contained `postgres:postgres` credentials.
5. `pytest tests` crashed outside the Airflow venv: `importorskip("airflow")` matched the repo's own `airflow/` folder.
6. The official Airflow image sets `PIP_USER=true`, which breaks `pip install` into the pipeline virtualenv. Fixed by inspection; not built.

## 5. GitHub Actions CI

The workflow is `.github/workflows/ci.yml` (`ubuntu-latest`, Python 3.12).

| Run | Commit | Trigger | Result |
|---|---|---|---|
| [41555834547](https://github.com/Dileep5/retail-data-lakehouse/actions/runs/41555834547) | `8abca04` | first push to `main`, 2026-11-03T00:51Z | **success**, both jobs |

| Job | Duration | Steps (all `success`) |
|---|---|---|
| `pipeline` | 4m30s (00:51:40 → 00:56:10 UTC) | Temurin Java 17 setup; install; lint; dependency checks (`pip check` + `pip-audit`, ignoring only the 5 documented sqlparse IDs); unit tests; end-to-end tests against a `postgres:16-alpine` service (Spark + GX + dbt); `dbt parse` |
| `airflow-dag` | 31s (00:51:41 → 00:52:12 UTC) | Airflow 3.3.2 install with official constraints; DAG integrity tests |

**Scope of this evidence:** the step results above come from the GitHub Actions API. Downloading job logs through the API requires repository admin rights, so the **exact test counts printed by CI were not retrieved** and are not claimed here; they are visible in the run's logs on GitHub. All exact counts in sections 1–4 come from local runs. The CI end-to-end step runs complete batches through `run_batch`, which includes `dbt seed/snapshot/run/test` and the 14 reconciliation checks; whether those passed in CI is implied by the step's success, not read from logs. `airflow dags test` (actual DAG execution) is **not** part of CI and was verified locally only (sections 1b and 2).

## Not verified

- **Docker / Docker Compose:** `docker build` / `docker compose up` were never run (Docker unavailable).
- **Full Airflow scheduler:** the scheduler/API server (`airflow standalone`) was never run; the DAG was executed with `airflow dags test` (in-process, all 7 tasks).
