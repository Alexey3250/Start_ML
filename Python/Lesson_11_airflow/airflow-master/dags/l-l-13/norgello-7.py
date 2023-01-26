from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def printi(ts, run_id, **kwargs):
    return print(f"task number is: {task_number}, and ts: {ts}, and run_id {run_id}")


with DAG(
    'norgello-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='first task in lesson №11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 20),
    catchup=True
) as dag:
    for task_number in range(20):
        m2 = PythonOperator(
            task_id=f'num_tasks_on_python{task_number}',
            python_callable=printi,
            op_kwargs = {"task number": task_number}
        )