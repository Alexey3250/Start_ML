from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def task_num(task_number):
    print(f"task number is: {task_number}")

with DAG(
    'cherepashchuk',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
description='hw1',
schedule_interval=timedelta(days=1),
start_date=datetime(2023, 1, 19),
catchup=False,
) as dag:
    def print_context(ts, run_id, **kwargs):
        print(ts, run_id, sep='\n')

    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id="hw_7_cherepashchuk_task_number_"+str(i),
                bash_command=f"echo {i}",
            )
        else:
            task = PythonOperator(
                task_id="hw_7_cherepashchuk_task_number_"+str(i),
                python_callable=print_context,
                op_kwargs={'task_number' : int(i)},
            )

