from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def print_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)

with DAG(
    'a-grohovskaja-9_hw_12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:



    t1 = PythonOperator(
        task_id='print_variable_is_startml',
        python_callable=print_variable
    )

    t1