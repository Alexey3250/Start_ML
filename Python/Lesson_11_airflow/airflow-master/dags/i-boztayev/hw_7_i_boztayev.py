from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_3',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Lesson 11, homework 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['i_boztayev']

) as dag:

    def print_task_number(ts, run_id, task_number):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)
        return "task number is printed"

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )


    # А вот так в Airflow указывается последовательность задач
    t2
