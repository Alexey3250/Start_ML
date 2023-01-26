from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        "hw_2_p-terentev-14",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        description='first DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 12, 12),
        catchup=False,
        tags=['DAG_2']
) as dag:
    a1 = BashOperator(
        task_id='pwd',
        bash_command='pwd'
    )

    def print_ds(ds):
        print(ds)

    a2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_ds
    )
    a1>>a2