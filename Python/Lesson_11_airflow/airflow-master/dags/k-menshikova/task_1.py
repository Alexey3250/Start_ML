from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

"""documentation"""

def print_ds(ds):
    print(ds)
    
with DAG(
    'hw_1_k-menshikova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failoure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='task 1',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['k-menshikova_1'],
    ) as dag:
    
    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds)
    
    t1 >> t2
