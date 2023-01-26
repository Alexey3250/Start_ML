from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def print_world(task_number: str):
    print(f"task number is: {task_number}")


with DAG(
        dag_id='dag_hw_3_d-perepelitsa-15',
        default_args=default_args,
        description='Задание hw_3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 11),
        catchup=False
) as dag:
    for i in range(30):
        if i <= 9:
            task1 = BashOperator(
                task_id='task_bash' + str(i),
                bash_command=f"echo {i}"
            )
        else:
            task2 = PythonOperator(
                task_id='task_python' + str(i),
                python_callable=print_world,
                op_kwargs={'task_number': i}
            )
    task1 >> task2