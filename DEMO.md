# Demo

A 10-minute walkthrough of the main features. It assumes `make install`, a `.env` with `SOURCE_DB_URL` / `WAREHOUSE_DB_URL`, and `JAVA_HOME` pointing at Java 17+. The outputs shown come from real runs.

```bash
set -a; source .env; set +a      # export the DB URLs for the snippets below
```

## 1. Run three daily batches plus an idempotent re-run

```bash
make reset    # start from empty databases; replaying old dates on a newer warehouse is refused by design
make demo
# batch 2025-11-01 succeeded
# batch 2025-11-02 succeeded
# batch 2025-11-03 succeeded
# re-run of batch 2025-11-03 succeeded (idempotent)
```

Each batch logs its steps with timings (`step silver done in 8.5s …`) and writes evidence to `lake/_metrics/batch_id=<date>/` (`silver.json`, `quality.json`, `reconciliation.json`).

## 2. Show the cleaning: raw input vs quarantine vs Silver

```bash
grep -E "not-an-email|^,|  [A-Z]" data/landing/2025-11-02/customers.csv | head -3   # raw defects
.venv/bin/python -c "
import pandas as pd
print(pd.read_parquet('lake/quarantine/customers/batch_id=2025-11-02')[['customer_id','email','_reject_reason']])
print(pd.read_parquet('lake/silver/customers').query('customer_id == 85')[['customer_id','email']])"
```
Expected:
- **Quarantined:** `not-an-email` → `invalid_email`, and a NULL id → `missing_customer_id`.
- **Normalised:** customer 85's `  ETHAN.ANDERSON.85@EXAMPLE.COM ` becomes `ethan.anderson.85@example.com` in Silver.

## 3. Prove idempotency with content fingerprints

```bash
.venv/bin/python -c "from lakehouse.fingerprint import fingerprint; import json, os; json.dump(fingerprint(os.environ['WAREHOUSE_DB_URL']), open('/tmp/fp.json','w'))"
.venv/bin/python -m lakehouse.cli run --batch-date 2025-11-03
.venv/bin/python -c "
from lakehouse.fingerprint import fingerprint; import json, os
b = json.load(open('/tmp/fp.json')); a = fingerprint(os.environ['WAREHOUSE_DB_URL'])
print(sum(list(v) == b[k] for k, v in a.items()), 'of', len(a), 'tables identical')"
# 9 of 9 tables identical
```

## 4. Out-of-order re-runs are refused

```bash
.venv/bin/python -m lakehouse.cli --log-level ERROR run --batch-date 2025-11-01
# ERROR (OutOfOrderBatchError): Batch 2025-11-01 is older than succeeded batch 2025-11-03; ...
```

## 5. SCD Type 2 and point-in-time attribution

```bash
.venv/bin/python -c "
import psycopg, os; c = psycopg.connect(os.environ['WAREHOUSE_DB_URL'])
for r in c.execute('select customer_id, version, loyalty_tier, valid_from, valid_to, is_current from analytics_marts.dim_customers where customer_id = 1 order by version'): print(r)
print(c.execute('select customer_resolution, count(*) from analytics_marts.fct_orders group by 1').fetchall())"
```
Expected:
- **Customer 1:** version 1 `Gold` (valid until 2025-11-01 09:30), then version 2 `Silver` (current).
- **`customer_resolution`:** almost all orders are `point_in_time`, plus 1 `unknown`: the latest day's late-arriving customer, re-attributed by the next batch.

## 6. Data-quality gate and recovery

```bash
TEST_PG_ADMIN_URL=postgresql://postgres:pw@localhost:5432/postgres \
  .venv/bin/python -m pytest tests/integration -q -k "quality_gate or late_arriving or older_batch"
```
These tests run against throwaway databases and check that:
- a CSV with 20% invalid emails fails the gate, the batch is marked `failed`, and the warehouse fingerprint is unchanged;
- the clean file re-run then succeeds;
- late line items and late customers are handled;
- out-of-order re-runs are refused.

The quality report shows 22/23 on batches after the first. The single non-pass is a **warning**, not a failure:

```bash
.venv/bin/python -c "
import json; q = json.load(open('lake/_metrics/batch_id=2025-11-02/quality.json'))
print([(c['entity'], c['column'], c['severity'], c['unexpected_count']) for c in q['checks'] if not c['success']])"
# [('orders', 'customer_id', 'warning', 1)]   <- the late-arriving customer, by design
```

## 7. Reports

```bash
make report && head -12 reports/analytics_report.md
```

## 8. Airflow DAG execution

```bash
make install-airflow
export AIRFLOW_HOME=$PWD/.airflow AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags AIRFLOW__CORE__LOAD_EXAMPLES=False \
       PIPELINE_PYTHON=$PWD/.venv/bin/python LAKEHOUSE_PROJECT_DIR=$PWD
.venv-airflow/bin/airflow db migrate
.venv-airflow/bin/airflow dags test retail_lakehouse 2025-11-04
# ... DagRun Finished: dag_id=retail_lakehouse, logical_date=2025-11-04 ..., state=success
make test-dag      # 3 DAG integrity tests
```
