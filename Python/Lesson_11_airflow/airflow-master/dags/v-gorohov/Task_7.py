from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'start_date': datetime(2022, 11, 18),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

#ts and run_id Airflow will take from context
def print_task_number(ts, run_id, **kwargs):
    print(ts)
    print(run_id)

with DAG(
    "hw_7_v-gorohov_cool_dag",
    default_args=default_args,
    schedule_interval=None
) as dag:

    for i in range(20):
        python_operator = PythonOperator(
            task_id=f"python_{i}",
            python_callable=print_task_number,
            op_kwargs={
                "task_number": f"{i}"
            },
            dag=dag
        )