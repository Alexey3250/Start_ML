from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def return_string():
    return "Airflow tracks everything"

def pull_string(ti):
    sample_xcom_key = ti.xcom_pull(
        key='return_value',
        task_ids='return_string'
    )

with DAG(
    'hw_10_v-kravtsova',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },

    description = 'hw_2',
    start_date = datetime(2022, 12, 23),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False

) as dag:
    t1 = PythonOperator(
        task_id='return_string',
        python_callable=return_string
    )

    t2 = PythonOperator(
        task_id='pull_string',
        python_callable=pull_string
    )

t1>>t2