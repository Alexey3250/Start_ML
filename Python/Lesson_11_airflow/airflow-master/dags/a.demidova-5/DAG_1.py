"""
First DAG
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    'first_dag',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },

    description='Very first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:

    date = "{{ ds }}"

    def get_date(ds):
        return print(ds)

    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print_date',
        python_callable=get_date
    )

    dag.doc_md = __doc__

    t1 >> t2
