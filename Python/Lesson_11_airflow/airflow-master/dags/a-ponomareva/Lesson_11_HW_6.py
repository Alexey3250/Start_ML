"""
Возьмите BashOperator из второго задания (где создавали task через цикл) и подбросьте туда переменную окружения NUMBER,
чье значение будет равно i из цикла. Распечатайте это значение в команде, указанной в операторе (для этого используйте bash_command="echo $NUMBER").
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'DAG_HW_6_ponomareva',
    default_args=default_args,
    description='DAG for HW_6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['ponomareva'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='print_command_' + str(i),
            env={'NUMBER': str(i)},
            bash_command=f'echo $NUMBER'
        )

        t1.doc_md = dedent(
            """\
            ### `BashOperator`
            Printing **number** of commands
            with *echo* command in cycle
            """
        )
