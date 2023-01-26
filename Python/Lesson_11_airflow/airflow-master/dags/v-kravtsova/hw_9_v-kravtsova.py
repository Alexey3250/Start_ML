from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def put_in_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )

def pull_and_print_xcom(ti):
    sample_xcom_key = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='put_in_xcom'
    )
    print(sample_xcom_key)

with DAG(
    'hw_9_v-kravtsova',
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
        task_id='put_in_xcom',
        python_callable=put_in_xcom
    )

    t2 = PythonOperator(
        task_id='pull_and_print_xcom',
        python_callable=pull_and_print_xcom
    )

t1>>t2


