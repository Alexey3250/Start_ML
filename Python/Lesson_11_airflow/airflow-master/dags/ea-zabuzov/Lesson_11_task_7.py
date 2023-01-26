from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'Lesson_11_step_7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='My second training DAG',
        start_date=datetime(2022, 6, 15),
        schedule_interval=timedelta(days=1),
        catchup=False,
        tags=['e.zabuzov', 'step_7']
) as dag:
    def print_task_number(ts, run_id, task_number, **kwargs):
        print(f'ts is {ts}')
        print(f'run_id is {run_id}')
        print(f'task number is: {task_number}')


    for i in range(20):
        t_py = PythonOperator(
                task_id=f'PythonOperator_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
        )