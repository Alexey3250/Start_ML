from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_3_p-tarabanov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    
    description='3 task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 9, 11),
    catchup=False,
    tags=['10BashOperator and 20PythonOperator'],
) as dag:
    
    for i in range(10):
        task1 = BashOperator(
            task_id=f'BashOperator_{i}',
            bash_command=f'echo {i}',
        )
        
    def print_number(task_number):
        print(f"task number is: {task_number}")
    
    for i in range(20):
        task2 = PythonOperator(
            task_id=f'PythonOperator_{i}',
            python_callable=print_number,
            op_kwargs={'task_number': i}
        )
    
    task1 >> task2
    