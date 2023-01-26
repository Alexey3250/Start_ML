from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def print_ds(ds):
    print(ds)
    return "This string have to be printed after ds print"


with DAG(
    "hw_2_s-matveev-9",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    task1_bash = BashOperator(
        task_id='hw2_msv_task1_bash',
        bash_command='pwd',
    )

    task2_python = PythonOperator(
        task_id='hw2_msv_task2_python',
        python_callable=print_ds,
    )

    task1_bash >> task2_python