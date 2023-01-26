from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'a-kamentsev_dag6',
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
    tags=['a-kamentsev_dag6'],
) as dag:
    for i in range(10):
        task = BashOperator(
            task_id = 'command_' + str(i),
            bash_command = 'echo $NUMBER',
            env = {'NUMBER': str(i)}
        )
        task