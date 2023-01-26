from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'DAV_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='DAV_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_3'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='bash_operators_' + str(i),
            bash_command=f"echo {i}",
        )


    def task_number(task_number):
        print(f"task number is: {task_number}")




    for i in range(20):
        t2 = PythonOperator(
            task_id='python_operators_' + str(i),
            python_callable=task_number,
            op_kwargs={'task_number': i}

