from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_7_p-tarabanov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    
    description='7 task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 9, 11),
    catchup=False,
    tags=['PythonOperator env NUMBER'],
) as dag:
    
    for i in range(10):
        task1 = BashOperator(
            task_id=f'BashOperator_{i}',
            bash_command=f'echo {i}',
        )
        
    def print_number(ts, run_id, **kwargs):
        print(f'ts is: {ts}')
        print(f'run_id is: {run_id}')
        print(f"task number is: {kwargs['task_number']}")
    
    for i in range(20):
        task2 = PythonOperator(
            task_id=f'PythonOperator_{i}',
            python_callable=print_number,
            op_kwargs={'task_number': i}
        )