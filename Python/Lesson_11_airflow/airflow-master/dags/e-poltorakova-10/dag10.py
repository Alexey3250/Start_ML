from airflow import DAG
from airflow.operators.python import PythonOperator
from textwrap import dedent
from datetime import datetime, timedelta
from airflow.models import Variable

with DAG(
    'hm_12_e-poltorakova',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['e-poltorakova'],

) as dag:

    def print_veriable():
        is_startml = Variable.get("is_startml") 
        print(is_startml)

    task = PythonOperator(
        task_id = 'print_veriable',
        python_callable=print_veriable,
    )

    task.doc_md = dedent(
        """
        Example of using variables
        """
        )

    task