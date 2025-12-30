from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="devcontainer_external_test_2",
    start_date=datetime(2024, 1, 1),
    schedule=None,      # manual trigger only
    catchup=False,
    tags=["dev", "external"],
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command='echo "Hello from Airflow container!" && date && whoami && pwd',
    )
