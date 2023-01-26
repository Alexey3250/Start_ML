from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable



with DAG(
    'Variable',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2023, 1, 21),
    tags=['a-rybakovskaya'],

) as dag:
    def print_variable():
        variable = Variable.get("is_startml")
        return print(variable)


    t1 = PythonOperator(
        task_id='print_variable',
        python_callable=print_variable)

    t1
