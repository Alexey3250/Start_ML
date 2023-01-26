from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'a-chernenko-30_DAG_hw7',
    # Параметры по умолчанию для тасок
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='a-chernenko-30_DAG_hw7',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 11, 23),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['hw_7'],
) as dag:


    def print_task_number(ts, run_id, task_number):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)
        # t1

    for i in range(30):
        if i < 10:
            bash_task = BashOperator(
                task_id = f'a-chernenko-30_DAG_hw7_{i}',
                env = {"NUMBER":f'{i}'},# id, будет отображаться в интерфейсе
                bash_command = "echo $NUMBER"
                )
        else:
            python_task = PythonOperator(
                task_id = f'print_python_task_{i}',
                python_callable = print_task_number,
                op_kwargs = {'task_number': i}
            )
    bash_task >> python_task