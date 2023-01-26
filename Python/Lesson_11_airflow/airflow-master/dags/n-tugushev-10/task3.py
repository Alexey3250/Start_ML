from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_(task_number):
    print(f"task number is: {task_number}")


with DAG(
    'n-tugushev-10-task3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='For loop',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 17),
    catchup=False,
    tags=['n-tugushev-10'],
) as dag:

    for i in range(10):
        bash = BashOperator(
            task_id='task_bash_' + str(i),
            bash_command=f"echo {i}",
        )

    for i in range(20):
        python = PythonOperator(
            task_id='task_python_' + str(i+10),
            python_callable=print_,
            op_kwargs={'task_number': i+10},
        )

bash >> python






