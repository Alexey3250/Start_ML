"""
hw_7_k-schipitsina
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'hw_7_k-schipitsina',
        # Параметры по умолчанию для тасок
        default_args={
            # Если прошлые запуски упали, надо ли ждать их успеха
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        # Описание DAG (не тасок, а самого DAG)
        description='hw_7_k-schipitsina (7 DAG)',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        # С какой даты начать запускать DAG
        # Каждый DAG "видит" свою "дату запуска"
        # это когда он предположительно должен был
        # запуститься. Не всегда совпадает с датой на вашем компьютере
        start_date=datetime(2022, 8, 18),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        # теги, способ помечать даги
        tags=['7'],
) as dag:
    dag.doc_md = """
    hw_7_k-schipitsina
    """

    def my_print_task_id(ts, run_id, **kwargs):
        print(f"task number is: {kwargs['task_number']}, {ts}, {run_id}")


    for i in range(11, 31):
        task_p = PythonOperator(
            task_id='task_number_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=my_print_task_id,
            op_kwargs={'task_number': i},
        )

    task_p
