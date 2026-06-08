from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/airflow/project")
DBT_PROJECT_DIR = f"{PROJECT_HOME}/dbt/italy_dwh"
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", f"{PROJECT_HOME}/.dbt")

with DAG(
    dag_id="italy_plan_daily_load",
    description="Daily Excel plan load -> dbt plan-dependent marts -> dbt tests",
    start_date=datetime(2026, 2, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["italy", "duckdb", "dbt", "plan"],
) as dag:
    load_plan = BashOperator(
        task_id="load_plan_excel",
        bash_command=f"cd {PROJECT_HOME} && python scripts/load_plan.py",
    )

    dbt_run_plan = BashOperator(
        task_id="dbt_run_plan_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select stg_plan_daily+ mart_plan_fact_daily"
        ),
    )

    dbt_test_plan = BashOperator(
        task_id="dbt_test_plan_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --select stg_plan_daily+ mart_plan_fact_daily"
        ),
    )

    load_plan >> dbt_run_plan >> dbt_test_plan
