from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def get_cycle(task_number):
    print(f"task number is: {task_number}")

with DAG(
    'g-karateev_d5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'A simple DAG for task 2',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 5, 5),
    catchup = False,
    tags = ['example']
) as dag:
    for i in range(30):
        if i < 10:
            task = BashOperator(
                env = {"NUMBER": i},
                task_id = f'echo_i_{i + 1}',
                bash_command = "echo $NUMBER"
            )
        else:
            task = PythonOperator(
                task_id = f'print_task_number_{i - 9}',
                python_callable = get_cycle,
                op_kwargs = {'task_number': str(i)}
            )
