from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


with DAG(
    "hw_6_s-matveev-9",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    for i in range(10):
        bash_task = BashOperator(
            task_id=f"hw6_msv_bash_task_{i}",
            bash_command="exho $NUMBER",
            env={"NUMBER": i}
        )