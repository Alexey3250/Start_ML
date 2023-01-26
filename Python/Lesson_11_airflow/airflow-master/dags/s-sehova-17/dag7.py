from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
    's-sehova-17-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),

    catchup=False,
    tags=['example'],
) as dag:
    
    def get_task_number(task_number, ts, run_id):
        print(f'task number: {task_number}')
        print(ts)
        print(run_id)


    for i in range(20):
        t1 = PythonOperator(
            task_id='task_number' + str(i),
            python_callable=get_task_number,
            op_kwargs={'task_number': i, }
        )
        
t1
