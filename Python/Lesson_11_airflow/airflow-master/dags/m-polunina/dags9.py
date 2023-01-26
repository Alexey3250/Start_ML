from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('polunina_9',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    # Описание DAG (не тасок, а самого DAG)
    description='A unit 9',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 20),
    catchup=False,
    tags=['A unit 9'],
) as dag:

    def push_xcom(ti):
        ti.xcom_push(
        key = "sample_xcom_key",
        value = "xcom test"
        )
    t1 = PythonOperator(task_id = 'python_9_1', python_callable = push_xcom)

    def print_ti(ti):
        test_ti = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids='python_9_1'
    )
        print(test_ti)
    t2 = PythonOperator(task_id = 'python_9_2', python_callable = print_ti)

    t1 >> t2



