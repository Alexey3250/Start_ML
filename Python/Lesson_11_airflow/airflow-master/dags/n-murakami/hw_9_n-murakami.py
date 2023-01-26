from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def push_value(ti):
    # в ti уходит task_instance, его передает Airflow под таким названием
    # когда вызывает функцию в ходе PythonOperator
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )

def pull_value(ti):
    result = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="write_to_xcom"
    )
    print(result)


with DAG(
    'hw_9_n-murakami',
    default_args={
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_9_n-murakami']
) as dag:
    t1 = PythonOperator(
        task_id="write_to_xcom",
        python_callable=push_value
    )
    t2=PythonOperator(
        task_id="take_from_xcom",
        python_callable=pull_value
    )
    t1 >> t2

