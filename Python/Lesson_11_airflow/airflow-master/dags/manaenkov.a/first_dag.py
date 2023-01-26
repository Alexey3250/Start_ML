from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


def print_ds(ds):

    print(ds)
    return "Woe, oh woe to my lands, I'm learning airflow"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_2_manaenkov',
    default_args=default_args,
    description='DAG for first task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:

    t1 = BashOperator(task_id="print_working_directory", bash_command="pwd")

    t2 = PythonOperator(task_id="get_logical_date", python_callable=print_ds)

    t1 >> t2
