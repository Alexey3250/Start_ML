from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'm-lysenko-11_task_4',
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
        description='m-lysenko-11_task_4',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        # С какой даты начать запускать DAG
        # Каждый DAG "видит" свою "дату запуска"
        # это когда он предположительно должен был
        # запуститься. Не всегда совпадает с датой на вашем компьютере
        start_date=datetime(2022, 1, 1),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        # теги, способ помечать даги
        tags=['m-lysenko-11_task_4'],
) as dag:
    def print_ds(task_number):
        print(f'task number is: {task_number}')
    for i in range(10):
        bash_task = BashOperator(
            task_id='m-lysenko-11_task_4_' + str(i+1),
            depends_on_past=False,
            bash_command=f"echo {i+1}",
        )
    for i in range(20):
        py_task = PythonOperator(
            task_id='m-lysenko-11_task_4_' + str(i + 11),
            python_callable=print_ds,
            op_kwargs={'task_number': i + 11}
        )
    bash_task.doc_md = dedent(
        """
        ### Hi bash
        for **10 tasks** *(from 1 to 10)*:
        `bash_command=f"echo {i+1}`
        """
        )
    py_task.doc_md = dedent(
        """
        ### Hi py
        for **20 tasks** *(from 11 to 30)*:
        `print(f'task number is: {task_number}')`
        """
    )

    bash_task >> py_task




