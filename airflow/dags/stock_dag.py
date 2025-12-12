from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# --- CONFIGURATION ---
PROJECT_DIR = "/home/roy1916/stock-pipeline"
VENV_PYTHON = f"{PROJECT_DIR}/venv/bin/python"

default_args = {
    'owner': 'roy1916',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DEFINE THE DAG ---
with DAG(
    'stock_automation_v1',
    default_args=default_args,
    description='A simple pipeline to ingest and clean stock data',
    schedule='@daily',              # <--- CHANGED THIS LINE
    catchup=False,
    tags=['stock', 'etl'],
) as dag:

    # TASK 1: Run Ingestion
    t1_ingest = BashOperator(
        task_id='ingest_stock_data',
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} ingest.py",
    )

    # TASK 2: Run Spark Cleaning
    t2_clean = BashOperator(
        task_id='clean_stock_data',
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} clean.py",
    )

    # --- SET DEPENDENCIES ---
    t1_ingest >> t2_clean
