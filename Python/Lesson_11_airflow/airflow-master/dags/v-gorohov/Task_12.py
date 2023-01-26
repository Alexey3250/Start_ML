from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.models import Variable
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

def pull_var():
    var = Variable.get("is_startml")
    print(var)

with DAG(
    "hw_12_v-gorohov_cool_dag",
    default_args=default_args,
    schedule_interval=None
) as dag:
    python_var = PythonOperator(
        task_id="push",
        python_callable=pull_var,
        dag=dag
    )