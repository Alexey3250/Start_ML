from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_ds(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'ds printed'

with DAG(

    'task2_m-vasilevskij',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date = datetime(2022, 12, 14),
) as dag:

    t1 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd'
    )

    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_ds
    )


    t1 >> t2





