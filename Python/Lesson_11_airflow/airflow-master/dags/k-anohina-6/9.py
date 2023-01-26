"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'AKA-9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    def set_xcom_test(ti):
        ti.xcom_push(key='sample_xcom_key',
                     value='xcom test')

    def print_i(ti):
        print(ti.xcom_pull(key='sample_xcom_key', task_ids='task_set'))
        return 'test_return'


    t1 = PythonOperator(
        task_id='task_set',
        python_callable=set_xcom_test
    )
    t2 = PythonOperator(
        task_id='task_get',
        python_callable=print_i,
    )

    t1 >> t2
