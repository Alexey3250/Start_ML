from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def get_variable():
    from airflow.models import Variable
    is_prod = Variable.get("is_prod")
    return is_prod


def get_connection():
    from airflow.hooks.base_hook import BaseHook

    connection = BaseHook.get_connection("postgres_main")
    conn_password = connection.password
    conn_login = connection.login
    return conn_password, conn_login


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'connections_and_variables',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    t1 = PythonOperator(
        task_id='example_variable',
        python_callable=get_variable,
    )
    t2 = PythonOperator(
        task_id='example_connection',
        python_callable=get_connection,
    )
    t1 >> t2
