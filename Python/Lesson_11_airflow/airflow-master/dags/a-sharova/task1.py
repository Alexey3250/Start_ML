"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    'dag1_sharova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='first dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='bash1pwd',
        bash_command='pwd',
    )

    def func1(ds, **kwargs):
        print(ds)

    t2 = PythonOperator(
        task_id='task1date',
        python_callable=func1,
    )

    t1 >> t2
