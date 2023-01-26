"""Homework_2 script"""

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'hw_2_d-gusev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG for hw_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 13),
        catchup=False,
        tags=['hw_2']
) as dag:
    do_pwd = BashOperator(
        task_id='do_pwd',
        bash_command='pwd',
    )

    do_pwd.doc_md = dedent(
        """
        В `BashOperator` выполните команду `pwd`, которая выведет директорию,
        где выполняется ваш код Airflow.
        """
    )

    def print_ds(ds):
        print(ds)

    print_date = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )

    print_date.doc_md = dedent(
        """
        В функции `PythonOperator` примите аргумент `ds` и распечатайте его.
        """
    )

    do_pwd >> print_date