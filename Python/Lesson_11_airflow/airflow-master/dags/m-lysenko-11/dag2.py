from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'm-lysenko-11_task_2',
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
        description='m-lysenko-11_task_2',
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
        tags=['m-lysenko-11_task_2'],
) as dag:
    def print_ds(ds):
        print(ds)
    py_task = PythonOperator(
        task_id='m-lysenko-11_task_2_1',
        python_callable=print_ds,
    )
    bash_task = BashOperator(
        task_id='m-lysenko-11_task_2_2',
        depends_on_past=False,
        bash_command='pwd',
    )

    bash_task >> py_task




