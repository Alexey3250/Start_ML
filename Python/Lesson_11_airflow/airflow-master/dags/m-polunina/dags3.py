from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('polunina_3',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    # Описание DAG (не тасок, а самого DAG)
    description='A unit 3',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 20),
    catchup=False,
    tags=['A unit 3'],
) as dag:

    for i in range(1, 11):
        t1 = BashOperator(task_id = 'bash_3_' + str(i), bash_command = f"echo {i}")

    def print_task(task_number):
        print(f"task number is: {task_number}")

    for i in range(11, 31):
        t2 = PythonOperator(task_id = 'python_3_' + str(i), python_callable = print_task, op_kwargs={'task_number': i})

    t1 >> t2

