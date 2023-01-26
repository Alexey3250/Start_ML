from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_task(task_number):
    print(f'task number is: {task_number}')


with DAG(
    'hw_2_1_a-telesh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW 2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 22),
    catchup=False,
    tags=['At']
) as dag:
    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id=f't_bash{i}',
                bash_command=f'echo {i}'
            )
        else:
            task = PythonOperator(
                task_id=f't_python{i}',
                python_callable=print_task,
                op_kwargs={'task_number': i}
            )
