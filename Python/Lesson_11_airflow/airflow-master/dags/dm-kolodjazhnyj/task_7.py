from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_task_number(ts, run_id, **kwargs):
    print(ts)
    print(run_id)
    print(f"task number is: {kwargs['task_number']}")

with DAG(
    'task_7_dm-kolodjazhny',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)},
    start_date=datetime(2022, 8, 9)
    ) as dag:

    for i in range(20, 30):
        task = PythonOperator(
            task_id=f'print_i_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )
    
    task