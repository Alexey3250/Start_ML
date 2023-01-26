from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'HW_7_d-lobachev_KWARGS',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG step 4 of HW Lesson 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 2, 10),
        catchup=False,
        tags=['lobachev'],
) as dag:
    def print_task(task_number, ts, run_id, **kwargs):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)


    for i in range(20):
        python_op = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task,
            op_kwargs={'task_number': i}
        )
