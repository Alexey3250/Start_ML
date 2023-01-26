from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta


with DAG(
    'firstdag',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
},
tags=['MaximovS']
) as dag:
    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd')


    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'


    t2 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context
    )
    t1 >> t2