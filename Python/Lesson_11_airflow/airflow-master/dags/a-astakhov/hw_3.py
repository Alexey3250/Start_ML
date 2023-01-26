from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_3_a-astakhov',
    # Параметры по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    tags=['hw_3_a-astakhov'],
) as dag:

    for i in range(10):
        bash_task = BashOperator(
            task_id=f'Bash_task_{i}',
            bash_command=f'echo {i}'
        )
        bash_task

    for i in range(20):
        # @task(task_id=f'Python_task_{i}')
        def py_task(task_number: int) -> None:
            print(f'task number is: {task_number}')

        python_task = PythonOperator(
            task_id=f'Python_task_{i}',
            python_callable=py_task,
            op_kwargs={'task_number': i}
            )
        python_task
