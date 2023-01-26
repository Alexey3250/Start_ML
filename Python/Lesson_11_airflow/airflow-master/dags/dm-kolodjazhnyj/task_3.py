from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_task_number(task_number):
    print(f"task number is: {task_number}")

with DAG(
    'task_2_dm-kolodjazhny',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)},
    start_date=datetime(2022, 8, 9)
    ) as dag:

    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id=f'echo{i}',  # id, будет отображаться в интерфейсе
                bash_command=f'echo{i}',  # какую bash команду выполнить в этом таске
            )
        else:
            task = PythonOperator(
                task_id=f'print_i_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )
    
    task