from datetime import datetime, timedelta
from textwrap import dedent
# To declare a DAG the DAG class of the airflow must be imported
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    "task_11.2",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes =5),
    },
    description='m-chajkovskij_11.2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['homework']
)as dag:
    def print_ds(ds, **kwargs):
        print(ds)
        print('This message should appear in the airflow log!')
    t1 = BashOperator(task_id='bash_11.2', bash_command='pwd',)
    t2 = PythonOperator(task_id='python_11.2', python_callable=print_ds,)

    t1 >> t2
