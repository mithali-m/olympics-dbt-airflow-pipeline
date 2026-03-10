from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "olympic_etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_DIR = "/opt/airflow"

with DAG(
    dag_id="olympic_etl_pipeline",
    description="Olympic ETL Pipeline — Ingest → dbt → Monitor",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["olympic", "etl"],
) as dag:

    # ── Step 1: Ingest raw data ──
    ingest = BashOperator(
        task_id="ingest_raw_data",
        bash_command=f"cd {PROJECT_DIR} && python ingestion/load_raw.py",
    )

    # ── Step 2: Run dbt staging ──
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"cd {PROJECT_DIR}/olympic_dbt/models && dbt run --select staging --profiles-dir /opt/airflow/olympic_dbt",
    )

    # ── Step 3: Run dbt dims ──
    dbt_dims = BashOperator(
        task_id="dbt_dimensions",
        bash_command=f"cd {PROJECT_DIR}/olympic_dbt/models && dbt run --select marts.dim_athlete marts.dim_country marts.dim_coach marts.dim_host marts.dim_sport marts.dim_medals marts.dim_event --profiles-dir /opt/airflow/olympic_dbt",
    )

    # ── Step 4: Run dbt facts ──
    dbt_facts = BashOperator(
        task_id="dbt_facts",
        bash_command=f"cd {PROJECT_DIR}/olympic_dbt/models && dbt run --select marts.fact_athlete_performance marts.fact_country_performance marts.fact_event_performance --profiles-dir /opt/airflow/olympic_dbt",
    )

    # ── Step 5: Run monitoring ──
    monitor = BashOperator(
        task_id="run_monitoring",
        bash_command=f"cd {PROJECT_DIR} && python monitoring/monitor.py",
    )

    # ── Pipeline order ──
    ingest >> dbt_staging >> dbt_dims >> dbt_facts >> monitor