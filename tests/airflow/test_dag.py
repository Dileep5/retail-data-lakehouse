"""DAG integrity tests. Run inside the Airflow environment:

    .venv-airflow/bin/python -m pytest tests/airflow -q
"""

import os
from pathlib import Path

import pytest

os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAG_FOLDER = Path(__file__).resolve().parents[2] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    try:  # Airflow 2.x
        return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)
    except TypeError:  # Airflow 3.x removed include_examples (controlled by LOAD_EXAMPLES)
        return DagBag(dag_folder=str(DAG_FOLDER))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_structure(dagbag):
    dag = dagbag.dags["retail_lakehouse"]
    assert dag is not None
    order = ["generate", "bronze", "silver", "quality", "load", "dbt", "reconcile"]
    assert sorted(t.task_id for t in dag.tasks) == sorted(order)
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}
    assert dag.max_active_runs == 1
    assert dag.catchup is False


def test_tasks_pass_logical_date_to_cli(dagbag):
    task = dagbag.dags["retail_lakehouse"].get_task("silver")
    assert "lakehouse.cli step silver --batch-date {{ ds }}" in task.bash_command
    assert task.retries == 1
