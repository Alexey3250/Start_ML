'''
Lesson 12. Task 2.
Напишите DAG, который будет содержать BashOperator и PythonOperator.
В функции PythonOperator примите аргумент ds и распечатайте его.
Можете распечатать дополнительно любое другое сообщение.
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def print_ds(ds, **kwargs):
    print(ds)
    print('Something else.')



with DAG(
    'k-shilin-15_task2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Task 2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 24),
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd'
        )
    t2 = PythonOperator(
        task_id = 'print_date',
        python_callable = print_ds
        )
    t1 >> t2