from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def print_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)


with DAG(
        'hw_12_a-shapovalov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Excercise 12 a-shapovalov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_12_a-shapovalov']
) as dag:

    task = PythonOperator(
        task_id='print_variable',
        python_callable=print_variable
    )

    task
