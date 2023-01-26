from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

default_args={
    'start_date': datetime(2022, 11, 18),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def branch_python():
    var = Variable.get("is_startml")
    print(var)
    if var == "True":
        return "startml_desc"
    return "not_startml_desc"

def true():
    print("StartML is a starter course for ambitious people")

def false():
    print("Not a startML course, sorry")

with DAG(
    "hw_12_v-gorohov_cool_dag",
    default_args=default_args,
    schedule_interval=None
) as dag:

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=branch_python,
        dag=dag,
    )

    startml = PythonOperator(
        task_id="startml_desc",
        python_callable=true,
        dag=dag
    )

    not_startml = PythonOperator(
        task_id="not_startml_desc",
        python_callable=false,
        dag=dag
    )

    branch >> [startml, not_startml]