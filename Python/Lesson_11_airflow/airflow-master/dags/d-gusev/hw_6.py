"""Homework_6 script"""

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'hw_6_d-gusev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG for hw_6',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 13),
        catchup=False,
        tags=['hw_6']
) as dag:
    for i in range(10):
        print_echo_count = BashOperator(
            task_id='print' + str(i),
            bash_command=f"echo $NUMBER",
            env={'NUMBER': str(i)}
        )

    print_echo_count.doc_md = dedent(
        """
        Возьмите *BashOperator* из второго задания
        (где создавали task через цикл) и
        подбросьте туда переменную **окружения** 'NUMBER',
        чье значение будет равно 'i' из цикла.
        *Распечатайте* это значение в команде,
        указанной в операторе (для этого используйте
        'bash_command="echo $NUMBER"').
        """
    )

    print_echo_count
