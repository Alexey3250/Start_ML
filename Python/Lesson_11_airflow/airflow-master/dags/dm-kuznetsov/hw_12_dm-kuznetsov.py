from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
        'dm-kuznetsov_hw_12',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='dm-kuznetsov_hw_12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 3),
        catchup=False,
        tags=['task_1'],
) as dag:

    def print_variable():
        is_startml = Variable.get("is_startml")
        print(is_startml)

    t1 = PythonOperator(
            task_id='print_var',
            python_callable=print_variable,
    )

    t1