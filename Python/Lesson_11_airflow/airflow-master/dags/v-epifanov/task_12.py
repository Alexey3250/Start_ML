from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.models import Variable


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def print_variable():
    res = Variable.get('is_startml')
    print(res)


with DAG(
    'task_12_vepifanov',
    description='vepifanov, задание 11.12',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['vepifanov'],
) as dag:

    t_1 = PythonOperator(
        task_id=f"print_variable",
        python_callable=print_variable,
    )

    t_1

