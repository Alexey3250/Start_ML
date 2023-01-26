from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def print_var():
    from airflow.models import Variable
    print(Variable.get("is_startml"))


with DAG(
    "hw_12_s-matveev-9",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    print_var_task = PythonOperator(
        task_id="hw12_msv_task_print_var",
        python_callable=print_var,
    )

    print_var_task