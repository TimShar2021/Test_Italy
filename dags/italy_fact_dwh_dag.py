from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/airflow/project")
DBT_PROJECT_DIR = f"{PROJECT_HOME}/dbt/italy_dwh"
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", f"{PROJECT_HOME}/.dbt")

with DAG(
    dag_id="italy_fact_dwh_incremental",
    description="Incremental POS extract -> dbt DWH/marts -> dbt tests",
    start_date=datetime(2026, 2, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["italy", "duckdb", "dbt", "fact"],
) as dag:
    bootstrap_source_db = BashOperator(
        task_id="bootstrap_pos_source_db",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            f"PYTHONPATH={PROJECT_HOME}:{PROJECT_HOME}/scripts "
            f"python scripts/bootstrap_sources.py"
        ),
    )

    extract_pos_incremental = BashOperator(
        task_id="extract_pos_incremental",
        bash_command=f"cd {PROJECT_HOME} && python scripts/extract_pos_incremental.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    bootstrap_source_db >> extract_pos_incremental >> dbt_run >> dbt_test
