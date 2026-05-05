from __future__ import annotations
from datetime import datetime
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator

SCRIPT_PATH = "/opt/airflow/python/consumer.py"

def run_consumer_script():
    try:
        result = subprocess.run(
            ["python", SCRIPT_PATH],
            capture_output=True,
            text=True,
            check=True
        )
        print("Output:", result.stdout)

    except subprocess.CalledProcessError as e:
        print("Error Output:", e.stderr)
        raise e


default_args = {
    "depends_on_past": False,
}

with DAG(
    dag_id="consumer_every_10min",
    description="Run consumer.py every 10 minutes inside Docker",
    default_args=default_args,
    start_date=datetime(2026, 5, 4),
    schedule="*/10 * * * *",    
    catchup=False,
    max_active_runs=1,
    tags=["consumer", "kafka"],
) as dag:

    task_run_consumer = PythonOperator(
        task_id="run_consumer_task",
        python_callable=run_consumer_script,
    )
