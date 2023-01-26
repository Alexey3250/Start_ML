from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def print_value_variable():
    from airflow.models import Variable

    is_startml = Variable.get("is_startml")
    print(is_startml)

with DAG(
        'hw_12_d-gusev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG for hw_12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 13),
        catchup=False,
        tags=['hw_12']
) as dag:

    print_variable = PythonOperator(
        task_id='print_value_variable',
        python_callable=print_value_variable,
    )

    print_variable.doc_md = dedent(
        """
        ## Напишите DAG, состоящий из одного `PythonOperator`.
        Этот оператор должен печатать значение `Variable` с названием `is_startml`.
        """
    )

    print_variable