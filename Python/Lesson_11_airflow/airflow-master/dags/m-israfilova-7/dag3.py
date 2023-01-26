from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'my4thdag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    }, 
    description='Just for practice',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=[ 'masha' ],
) as dag:
    def pushdata(ti):
        ti.xcom_push(key="sample_xcom_key", value="xcom test")

    def pulldata(ti):
        print(ti.xcom_pull(task_ids = 'taskpush', key='sample_xcom_key'))

    taskpush = PythonOperator(
        task_id = 'pushing',
        python_callable=pushdata,
        )
    taskpull = PythonOperator(
        task_id = 'pulling',
        python_callable=pulldata,
        )
taskpush >> taskpull