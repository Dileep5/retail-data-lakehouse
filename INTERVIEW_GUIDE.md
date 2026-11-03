# Interview Guide: Retail Data Lakehouse

Answers point to the code (`file:function`) and to measured results in `TEST_RESULTS.md`.

## Architecture

**1. Describe the pipeline end to end.**
For a batch date D:
1. **Generate:** `generate` writes CSV exports and OLTP rows.
2. **Bronze:** `bronze` registers the batch and computes its incremental window, then lands the CSVs and the windowed rows from `sales.orders` and `sales.order_items` as raw-string Parquet under `batch_id=D`.
3. **Silver:** PySpark types, normalises, validates (failures go to quarantine), deduplicates, then replaces, overwrites or upsert-merges Silver Parquet.
4. **Quality:** Great Expectations gates the batch.
5. **Load:** one-transaction upserts into `lake_silver`.
6. **dbt:** `seed`, `snapshot` (SCD2), `run` (dims and incremental facts), then `test`.
7. **Reconcile:** 14 cross-layer checks, then the batch is marked `succeeded`.

**2. Why both Bronze and Silver? Why keep Bronze as strings?**
Bronze is the replayable record of exactly what the source delivered. If cleaning logic changes, Silver can be rebuilt without re-extracting from the source. Storing Bronze as strings means a type change at the source never breaks ingestion; typing errors surface in Silver as quarantined rows with a reason, rather than as a crashed ingest.

**3. Why PySpark for this data volume?**
The data fits in pandas, honestly. Spark is used because the transformations are written as distributed DataFrame code (window functions, dynamic partition overwrite, left-semi joins), which would carry over to a cluster unchanged. The trade-off is ~8 s of JVM and Spark overhead per batch locally.

**4. Why PostgreSQL as both source and warehouse?**
It is a local demo. The source (`retail_source`) and the warehouse (`retail_dw`) are separate databases, and the code reaches them only through `SOURCE_DB_URL` and `WAREHOUSE_DB_URL`, so they could live on different servers.

## Incremental processing and idempotency

**5. How does incremental extraction work?**
`bronze.compute_window` is a pure function over the rows in `meta.ingestion_batches`:
- **new batch:** the window starts where the latest *succeeded* earlier batch ended, and ends at `D 00:00`;
- **re-run of a known batch:** it reuses that batch's recorded window.

`ingest_postgres` selects `updated_at > start AND updated_at < end`, so updates are caught as well as inserts. Measured: batch 2025-11-02 extracted 21 orders, of which 4 were late status changes on older orders.

**6. Why record windows instead of using `max(updated_at)` as the watermark?**
A max-based watermark moves every time the job runs. Re-running the same batch later would then extract a *different* set of rows. A recorded window makes re-runs deterministic, and anchoring on the last *succeeded* batch means a failed batch's changes are automatically picked up by the next one (`test_failed_batch_data_is_caught_up_by_next_batch`).

**7. Show me idempotency at each layer.**
| Layer | Mechanism |
|---|---|
| Bronze | `_write_partition` deletes and rewrites `batch_id=D` |
| Silver | `upsert_parquet` (latest `updated_at` wins), `replace_dataset`, dynamic partition overwrite |
| Warehouse | `INSERT … ON CONFLICT DO UPDATE`, or replace for snapshots, in one transaction |
| dbt | facts use `delete+insert` on unique keys; the snapshot only adds versions when `updated_at` advances |

Verified: re-running 2025-11-03 left all 9 warehouse tables with identical row counts and MD5 content hashes (`fingerprint.py`).

**8. Why refuse to re-run an older batch?**
Customers and products arrive as full snapshots and are replaced wholesale. Replaying 2025-11-01 after 2025-11-03 would roll `lake_silver.customers` back two days and break the dimension-coverage reconciliation. `register_batch` raises `OutOfOrderBatchError` before doing any work. Backfills require a rebuild in order. The audit also found that the refusal was marking the old batch `failed`; that is fixed and tested.

**9. Explain `upsert_parquet`.**
1. Reads the target, limited to the partitions touched by the update via a `left_semi` join on `order_month`.
2. Unions it with the updates tagged `_incoming = 1`.
3. Takes `row_number()` over the keys ordered by `updated_at DESC, _incoming DESC`, so the latest version wins and ties go to the new data.
4. Writes to a staging directory.
5. Swaps in only the leaf partition directories.

Spark can't overwrite a path it is lazily reading, which is why staging is needed. A test proves that untouched partitions keep their original files.

**10. What would you change on S3 or ADLS?**
Use Delta Lake or Iceberg `MERGE INTO`. That gives atomic commits across partitions, time travel and concurrent writers. The directory swap is atomic only per directory and assumes a single writer.

## Data quality

**11. How is bad data handled in Silver?**
`_apply_rules` evaluates an ordered list of named conditions. `coalesce(cond, false)` makes a NULL count as a failure, and `_reject_reason` records the first failing rule. Rejected rows go to `quarantine/<entity>/batch_id=D/`. Malformed values become NULL (ANSI mode off) and are then caught by a rule, so one bad string can never crash the job. Normalisable defects are fixed rather than rejected: e-mail case and whitespace, province case, `" Completed "`, `IN-STORE`.

**12. What does Great Expectations add on top of the Silver rules?**
Silver rules are row-level. GX checks **dataset-level** properties: uniqueness after dedup, compound-key uniqueness, value sets, numeric ranges, and cross-entity referential checks against Silver customers and products. Expectations are tagged critical or warning, and on top of that there is a quarantine-*rate* gate (> 5% fails the batch). The gate runs **before** the load. The e2e test corrupts 20% of emails and asserts that the batch fails, is marked `failed`, and leaves the warehouse fingerprint unchanged.

**13. Why is the customer foreign-key check only a warning?**
Because late-arriving customers are legitimate. An order can arrive a day before its customer's CRM record. Failing the batch would block all data for one expected lag. Instead it is surfaced as a warning, handled in dbt, and covered by a dbt test that fails if any order stays unresolved for more than 2 days.

**14. How is deduplication made deterministic?**
`_dedupe` orders by `updated_at DESC` and then by *every other column*. Two rows tied on the key and timestamp therefore always resolve the same way, which matters for the content-identical re-run guarantee.

**15. What do the 14 reconciliation checks prove?**
- **Extraction (2):** Bronze rows equal source rows in the window.
- **Silver accounting (5):** bronze = quarantined + duplicates + written.
- **Load (5):** Silver rows equal `lake_silver` rows.
- **Revenue (1):** revenue recomputed independently from Silver Parquet, using Python `Decimal` with the same half-up rounding as the dbt macro, matches `fct_orders` within one cent.
- **Dimension coverage (1):** current dimension members equal Silver customers.

Each layer boundary is checked by a separate code path, so no single bug can silently make everything agree.

## dbt modelling

**16. How is SCD Type 2 implemented?**
`dbt/snapshots/customers_snapshot.yml` uses the YAML snapshot config with `strategy: timestamp` on the CRM's `updated_at`. `dbt_valid_from`/`dbt_valid_to` therefore reflect *business* time, not when the pipeline ran. `dim_customers` exposes `customer_sk` (the `dbt_scd_id`), `valid_from`/`valid_to`, `is_current` and a version number. Tests check for no overlapping versions, exactly one current version per customer, and contiguous intervals (e2e).

**17. Why timestamp strategy rather than check strategy?**
Check strategy stamps versions with the run time. Replaying history or running late would put the wrong validity dates on every version. The CRM's `updated_at` is the true change time.

**18. How do facts use the SCD2 dimension?**
`fct_orders` joins point-in-time: `order_date >= valid_from::date AND (valid_to IS NULL OR order_date < valid_to::date)`. `rpt_revenue_by_tier_at_order` then shows revenue by the tier the customer held when ordering, compared with their tier today. That question can't be answered without SCD2.

**19. Walk me through late-arriving data handling.**
Two real bugs drove this; both were reproduced before fixing.
1. **Late line items.** The incremental `fct_orders` only recomputed orders from the new batch, so an item arriving later for an old order left its revenue stale. Fix: the incremental set also includes `order_id`s that received new `stg_order_items` rows.
2. **Late customers.** An order whose customer wasn't in the CRM export yet got `customer_sk = NULL`, failing `not_null` and halting the pipeline. Fix:
   - an unknown member (`'-1'`) in `dim_customers`;
   - resolution order: point-in-time → first version → unknown, recorded in `customer_resolution`;
   - incremental runs also reprocess orders currently attributed to `'-1'`.

Verified: 2025-11-03's late customer moved from `unknown` to `point_in_time` when the 2025-11-04 batch ran through Airflow.

**20. Why `delete+insert` and not `merge` for incremental models?**
It is supported on every PostgreSQL version that dbt-postgres supports, and it is naturally idempotent for re-runs. Rows for the affected keys are replaced wholesale, which is exactly the semantics needed when an order's revenue or attribution changes.

**21. How is the incremental watermark defined in dbt?**
`max(_batch_id)` of the existing fact, with `>=` rather than `>`. The latest batch is always reprocessed, so re-running it replaces its rows instead of skipping them.

**22. What singular tests did you write and why?**
- **SCD2:** no overlapping versions; exactly one current version per customer.
- **Reconciliation:** revenue matches staging; fact row counts match staging.
- **Grain and sanity:** unique inventory grain; no negative revenue.
- **Business rules:** no orders before a store opened. This test caught a generator bug: Halifax received orders before it opened, with 98 rows failing.
- **Late arrivals:** no orders stuck as `unknown` for more than 2 days.

**23. Why is `line_revenue` a macro?**
It is the single definition of net revenue (`quantity × unit_price × (1 − discount/100)`, rounded to cents). Staging uses it, and the Python reconciliation reimplements exactly the same rounding. With two definitions, a rounding difference would make the reconciliation flaky.

## Orchestration and operations

**24. Why does each Airflow task shell out to a CLI in another virtualenv?**
Airflow 3 is installed with an official constraints file that pins hundreds of packages, and adding PySpark, dbt and GX to it creates conflicts. The DAG instead runs `PIPELINE_PYTHON -m lakehouse.cli step <name> --batch-date {{ ds }}`. The same CLI runs locally and in CI, so what Airflow executes is exactly what was tested.

**25. Why `max_active_runs=1`?**
Each batch's window depends on the previous batch's recorded end. Concurrent runs could compute overlapping or out-of-order windows.

**26. How do failures surface?**
- **CLI:** each step raises a typed error (`DataQualityError`, `ReconciliationError`, `DbtError`, `OutOfOrderBatchError`, `ConfigurationError`). `cli.run` prints a one-line reason to stderr and exits 1, so Airflow marks the task failed and retries once.
- **Batch metadata:** `run_batch` logs the failing step with its duration and marks the batch `failed`, except for out-of-order refusals.
- **Per-batch evidence:** metrics JSON (Silver, quality, reconciliation) is written for every batch.

**27. How does recovery work after a bad file?**
The quality gate fails before the load, so the warehouse is unchanged and the batch is marked `failed`. Once the source re-delivers the file, re-running the same date succeeds. `test_quality_gate_blocks_bad_batch_and_recovers` tests exactly that sequence.

**28. How did you verify the DAG, not just parse it?**
- **Parsing:** DagBag tests check for no import errors, the 7-task chain, `max_active_runs`, `catchup` and the templated command.
- **Execution:** `airflow dags test retail_lakehouse 2025-11-04` ran all 7 tasks against the real PostgreSQL database and Spark, and the DagRun finished `success`.
- **What I did not verify:** the scheduler and webserver (`airflow standalone`) and the Docker images, because Docker wasn't available.

**29. How is the synthetic data made realistic and reproducible?**
- **Reproducible:** `simulate(D)` replays days from the start with per-day seeds, so a batch is a pure function of its date (tested at byte level).
- **Realistic:** it models SCD2 changes, new sign-ups, price changes, late status changes, late items, late customers, store opening dates, and a specific defect mix for Silver to clean.

**30. What would break first at 100× volume?**
In order:
1. The Python reconciliation and GX checks, which use `toPandas` / `read_parquet` on the batch and on full Silver for counts. These should run as Spark aggregations.
2. The single-transaction load. Use staged bulk loads per entity.
3. Plain-Parquet merges, which should move to Delta or Iceberg.
4. Local Spark, which should move to a cluster.

The dbt models are already incremental.

**31. What dependency risks did you find?**
- **Fixed by upgrading:** pip-audit flagged pyarrow 21 (upgraded to 23.0.1; all 56 tests re-run and passing) and pytest (upgraded to 9.x).
- **Accepted:** sqlparse has advisories fixed in 0.6.0, but dbt-core 1.10 pins it below 0.5.5. It only parses our own SQL. CI ignores exactly those IDs, so any new advisory still fails the build.

**32. What was the most instructive bug?**
The late-arriving customer. Every test passed on the original 2-batch scenario because the generator never produced a late CRM record. Once it did, a single realistic event stopped the whole pipeline on a `not_null` test. The lesson: test data has to contain the failure modes production will contain, and dbt tests protect correctness only if the models handle the cases those tests encode.
