from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_n(ts, run_id, **kwargs):
    print(kwargs['task_number'])
    print(ts)
    print(run_id)

with DAG(
        'step_7_mishura',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

        description='step_7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 20),
        catchup=False,
        tags=['step_2'],
) as dag:
    for i in range(30):
        if i < (10):
            task = BashOperator(
                task_id=f'{i} task',
                bash_command=f'echo {i}'
            )

        else:
            task = PythonOperator(
                task_id=f'print_smth_{i}',
                python_callable=print_n,
                op_kwargs={'task_number': i}
            )


        task