"""Daily retail lakehouse pipeline.

Each task runs one idempotent pipeline step through the CLI in a dedicated Python
environment (PIPELINE_PYTHON), which keeps PySpark/dbt/Great Expectations dependencies
out of Airflow's own constrained environment. The batch date is Airflow's logical date
(``ds``), so clearing and re-running a DAG run reprocesses exactly that batch.

    generate -> bronze -> silver -> quality -> load -> dbt -> reconcile
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator

    from airflow import DAG

PIPELINE_PYTHON = os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")
PROJECT_DIR = os.environ.get("LAKEHOUSE_PROJECT_DIR", "/opt/lakehouse")
STEPS = ["generate", "bronze", "silver", "quality", "load", "dbt", "reconcile"]

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="retail_lakehouse",
    description="CSV + PostgreSQL -> Bronze -> Silver (PySpark) -> PostgreSQL -> dbt marts",
    start_date=datetime(2025, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  # incremental windows depend on the previous batch
    default_args=default_args,
    tags=["lakehouse", "retail", "dbt", "spark"],
) as dag:
    previous = None
    for step in STEPS:
        task = BashOperator(
            task_id=step,
            bash_command=(
                f"cd {PROJECT_DIR} && {PIPELINE_PYTHON} -m lakehouse.cli step {step} --batch-date {{{{ ds }}}}"
            ),
        )
        if previous is not None:
            previous >> task
        previous = task
