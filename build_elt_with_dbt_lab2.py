from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

# Paths inside the Airflow container
DBT_PROJECT_DIR = "/opt/airflow/dbt/stock_lab2_dbt"   # folder that has dbt_project.yml
DBT_PROFILES_DIR = "/opt/airflow/dbt"                 # folder that has profiles.yml

# Get Snowflake connection so we can pass credentials to dbt via env vars
conn = BaseHook.get_connection("snowflake_conn")

with DAG(
    dag_id="BuildELT_dbt",
    start_date=datetime(2025, 3, 19),
    schedule="30 3 * * *",
    catchup=False,
    description="Run dbt models (run, test, snapshot) from Airflow",
    default_args = {
        "env": {
            "DBT_USER": str(conn.login or ""),
            "DBT_PASSWORD": str(conn.password or ""),
            "DBT_ACCOUNT": str(conn.extra_dejson.get("account", "")),
            "DBT_SCHEMA": "analytics",
            "DBT_DATABASE": str(conn.extra_dejson.get("database", "")),
            "DBT_ROLE": str(conn.extra_dejson.get("role", "")),
            "DBT_WAREHOUSE": str(conn.extra_dejson.get("warehouse", "")),
            "DBT_TYPE": "snowflake",
        },
    }
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "/home/airflow/.local/bin/dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "/home/airflow/.local/bin/dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            "/home/airflow/.local/bin/dbt snapshot "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_run >> dbt_test >> dbt_snapshot
