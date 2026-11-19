
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Project configuration
PROJECT_DIR = "/home/mawuko/nyc_taxi_etl"

ETL_PYTHON = f"{PROJECT_DIR}/.venv/bin/python"

# Define the Airflow DAG
with DAG(
    dag_id="nyc_taxi_etl_pipeline",
    description="NYC Taxi PySpark ETL Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args={
        "owner": "data_engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["nyc-taxi", "pyspark", "etl"],
) as dag:

    run_nyc_taxi_etl = BashOperator(
        task_id="run_nyc_taxi_etl",
        bash_command=f"cd {PROJECT_DIR} && {ETL_PYTHON} main.py",
        env={
            "JAVA_HOME": "/usr/lib/jvm/java-21-openjdk-amd64",
            "PATH": "/usr/lib/jvm/java-21-openjdk-amd64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        },
    )