"""
Airflow DAG: orchestrates the daily credit card transaction pipeline.

    generate_synthetic_data  (stand-in for a real ingestion source in prod)
            │
            ▼
    spark_transform          (clean, dedupe, engineer features)
            │
            ▼
    load_to_snowflake        (load cleaned data into RAW schema)
            │
            ▼
    dbt_run                  (build staging + mart models)
            │
            ▼
    run_data_quality_checks  (Great Expectations validation on the mart layer)

If run_data_quality_checks fails, the DAG fails loudly rather than letting
bad data silently reach the dashboard -- this "quality gate before BI"
pattern is the core idea the project is meant to demonstrate.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "analytics_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="credit_card_transaction_pipeline",
    description="Daily ingest -> transform -> load -> model -> validate -> BI pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["credit-card", "analytics-engineering", "portfolio-project"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_synthetic_data",
        bash_command="python {{ params.repo_root }}/data/generate_synthetic_data.py",
        params={"repo_root": "/opt/airflow/credit-card-pipeline"},
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command="python {{ params.repo_root }}/src/ingest/spark_transform.py",
        params={"repo_root": "/opt/airflow/credit-card-pipeline"},
    )

    load_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command="python {{ params.repo_root }}/src/ingest/load_to_snowflake.py",
        params={"repo_root": "/opt/airflow/credit-card-pipeline"},
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd {{ params.repo_root }}/dbt_project && dbt run",
        params={"repo_root": "/opt/airflow/credit-card-pipeline"},
    )

    data_quality_checks = BashOperator(
        task_id="run_data_quality_checks",
        bash_command="python {{ params.repo_root }}/src/quality/ge_validation_suite.py",
        params={"repo_root": "/opt/airflow/credit-card-pipeline"},
    )

    generate_data >> spark_transform >> load_snowflake >> dbt_run >> data_quality_checks
