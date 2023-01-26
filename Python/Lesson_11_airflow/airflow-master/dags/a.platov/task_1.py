from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'tutorial',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='First task',
        start_date=datetime(2022, 7, 17),
        catchup=False,
        tags=['first_task'],
    ) as dag:
    
        t_bash = BashOperator(
            task_id='print_pwd',  # id, будет отображаться в интерфейсе
            bash_command='pwd',  # выполняем комманду pwd
            dag=dag,
        )
        
        def print_context(ds):
            print(ds)

            
        t_python = PythonOperator(
            task_id='print_ds',
            python_callable=print_context,
            dag=dag,
        )
        
        # Указывается последовательность задач
        t_bash >> t_python