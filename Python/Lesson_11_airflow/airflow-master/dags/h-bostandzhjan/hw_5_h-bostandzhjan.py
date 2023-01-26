"""
dag for environment variable printing
"""
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('hw_5_h-bostandzjan',
         default_args=default_args,
         description='A simple tutorial DAG',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2021, 1, 1),
         catchup=False,
         tags=['hristo']) \
        as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id = 't_1_echo_the_' + str(i),
            bash_command = f'echo $NUMBER',
            env = {"NUMBER": str(i)}
        )

        t1.doc_md = dedent(
        """
        ### BashOperator
        В таске_1 распечатывается при помощи команды 'echo $NUMBER'
        подряд 10 чисел переменной окружения, чье значение задается i
        """
        )

    dag.doc_md = __doc__

    t1