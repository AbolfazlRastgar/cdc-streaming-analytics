# from __future__ import annotations

# from datetime import datetime
# import os

# from airflow import DAG
# from airflow.operators.python import PythonOperator

# SCRIPT_PATH = r"D:\final-project\python\user-generator.py"


# def run_user_generator():
#     # اجرای همان فایل پایتون به شکل سیستم‌عامل
#     # چون PythonOperator برای import کردن اسکریپت طراحی شده،
#     # اجرای مستقیم با os.system/ subprocess معمولاً مطمئن‌تر است.
#     import subprocess

#     subprocess.check_call(["python", SCRIPT_PATH])


# default_args = {
#     # جلوی اجرای دوباره قدیمی‌ها را می‌گیرد (در دمو عالی است)
#     "depends_on_past": False,
# }

# with DAG(
#     dag_id="user_generator_every_1min",
#     description="Run user-generator.py every 1 minute",
#     default_args=default_args,
#     start_date=datetime(2026, 5, 3, 10, 0),
#     schedule="*/1 * * * *",  # هر 1 دقیقه
#     catchup=False,            # برای اینکه از گذشته backfill نکند
#     max_active_runs=1,
#     tags=["demo", "data-engineering"],
# ) as dag:

#     task_run = PythonOperator(
#         task_id="run_user_generator",
#         python_callable=run_user_generator,
#     )



from __future__ import annotations
from datetime import datetime
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator

# در داکر، مسیر باید مسیر داخلی کانتینر باشد
SCRIPT_PATH = "/opt/airflow/python/user-generator.py"

def run_user_generator():
    # اجرای اسکریپت با پایتونِ داخل کانتینر
    try:
        result = subprocess.run(["python", SCRIPT_PATH], capture_output=True, text=True, check=True)
        print("Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error Output:", e.stderr)
        raise e

default_args = {
    "depends_on_past": False,
}

with DAG(
    dag_id="user_generator_every_1min",
    description="Run user-generator.py every 1 minute inside Docker",
    default_args=default_args,
    start_date=datetime(2026, 5, 3),
    schedule="*/1 * * * *", 
    catchup=False,
    max_active_runs=1,
    tags=["demo"],
) as dag:

    task_run = PythonOperator(
        task_id="run_user_generator_task",
        python_callable=run_user_generator,
    )