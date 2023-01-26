from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_4_o-murashova-13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),},
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 11),
    tags=['example'],
) as dag:
    def print_(task_number):
        return f"task number is: {task_number}"


    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f'bashik_{i}',
                bash_command=f"echo {i}",
                doc_md=dedent(
                    """ # Text task 1
                    text `print()' text
                    text *text*
                    **text**
                    """
                ),
            )
        else:
            t2 = PythonOperator(
                task_id=f'pitosha_{i}',
                python_callable=print_,
                op_kwargs={'task_number': i},
                doc_md=dedent(
                    """ # Text task 2
                    text `print()' text
                    text *text*
                    **text**
                    """),
            )

t1 >> t2
