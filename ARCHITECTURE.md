# Architecture

## Data flow

```mermaid
flowchart LR
    subgraph Sources
        CSV[Daily CSV exports<br/>customers, products, inventory<br/>data/landing/DATE/]
        OLTP[(PostgreSQL retail_source<br/>sales.orders, sales.order_items<br/>with updated_at)]
    end

    subgraph Lake[Parquet lake - LAKE_ROOT]
        BR[bronze/entity/batch_id=DATE<br/>raw strings + _source, _batch_id]
        SV[silver/entity<br/>typed, cleaned, deduplicated]
        QU[quarantine/entity/batch_id=DATE<br/>rows + _reject_reason]
        ME[_metrics/batch_id=DATE<br/>silver.json, quality.json,<br/>reconciliation.json]
    end

    CSV -->|pandas, dtype=str| BR
    OLTP -->|updated_at in window| BR
    BR -->|PySpark clean_*| SV
    BR --> QU
    SV --> GX{Great Expectations}
    GX -->|critical failure or<br/>quarantine rate > 5%| STOP[batch failed,<br/>warehouse untouched]
    GX --> LD[load: COPY → temp table → upsert<br/>one transaction]
    LD --> LS[(retail_dw.lake_silver)]

    subgraph dbt[dbt - retail_dw]
        STG[analytics_staging views]
        SNAP[analytics_snapshots.customers_snapshot<br/>SCD2, timestamp strategy]
        MART[analytics_marts<br/>dim_customers, dim_products, dim_stores, dim_date<br/>fct_orders, fct_order_items, fct_inventory_daily]
        RPT[analytics_reports views]
        STG --> MART
        SNAP --> MART --> RPT
    end
    LS --> STG
    LS --> SNAP
    MART --> REC{reconcile: 14 checks}
    REC --> META[(meta.ingestion_batches<br/>window, status, metrics)]
```

## Batch lifecycle and metadata

```mermaid
sequenceDiagram
    participant A as Airflow / CLI
    participant B as bronze.register_batch
    participant M as meta.ingestion_batches
    A->>B: batch D
    B->>M: read all batches
    alt a later batch already succeeded
        B-->>A: OutOfOrderBatchError (nothing changed)
    else D seen before
        B->>M: reuse D's window, status=running
    else new batch
        B->>M: window = (end of last succeeded, D 00:00], status=running
    end
    A->>A: bronze → silver → quality → load → dbt → reconcile
    alt any step fails
        A->>M: status=failed (data from its window is caught up by the next batch)
    else
        A->>M: status=succeeded, metrics
    end
```

## Orchestration

```mermaid
flowchart LR
    G[generate] --> B[bronze] --> S[silver] --> Q[quality] --> L[load] --> D[dbt<br/>seed, snapshot, run, test] --> R[reconcile]
```

- **DAG settings:** `airflow/dags/retail_lakehouse_dag.py` uses `schedule="@daily"`, `max_active_runs=1` (incremental windows must run in order), `catchup=False`, 1 retry, and a 30-minute task timeout.
- **How tasks run:** each task is a `BashOperator` that runs `python -m lakehouse.cli step <name> --batch-date {{ ds }}` in `PIPELINE_PYTHON`, a separate virtualenv. Airflow's constrained dependencies therefore never mix with PySpark, dbt or Great Expectations.
- **Failure signalling:** the CLI exits non-zero on failure, which marks the task failed.

## dbt model graph

```mermaid
flowchart LR
    S1[source lake_silver.customers] --> SNAP[customers_snapshot]
    S1 --> STGC[stg_customers]
    S2[lake_silver.orders] --> STGO[stg_orders]
    S3[lake_silver.order_items] --> STGI[stg_order_items]
    S4[lake_silver.products] --> STGP[stg_products]
    S5[lake_silver.inventory] --> STGV[stg_inventory]
    SEED[seed stores.csv] --> DS[dim_stores]
    SNAP --> DC[dim_customers + unknown member]
    STGP --> DP[dim_products]
    STGO & STGI & DC --> FO[fct_orders<br/>incremental]
    STGI & DP --> FI[fct_order_items<br/>incremental]
    STGV --> FV[fct_inventory_daily<br/>incremental]
    FO & DS --> R1[rpt_daily_sales]
    FO & FI --> R2[rpt_monthly_category_performance]
    DC & FO --> R3[rpt_customer_lifetime_value]
    FO & DC --> R4[rpt_revenue_by_tier_at_order]
    FV & DS & DP --> R5[rpt_inventory_reorder_alerts]
    DC --> R6[rpt_customer_change_history]
```

## Deployment (Docker Compose)

```mermaid
flowchart LR
    PG[(postgres:16-alpine<br/>init SQL creates retail_source,<br/>retail_dw, airflow)] --> AF[airflow standalone<br/>Dockerfile.airflow<br/>+ /opt/pipeline-venv]
    PG --> CLI[pipeline<br/>Dockerfile, profile 'cli']
    AF --- LAKE[(lake volume)]
    CLI --- LAKE
```
