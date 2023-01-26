from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_context(ds, **kwargs):
    print(ds)
    print('Is it really working?')

with DAG('hw_2_o_bulaeva_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries':1,
            'retry_delay': timedelta(minutes=5),
            },
        description = 'First DAG',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2022, 7, 24),
        catchup = False) as dag:
    t1 = BashOperator(
    	task_id = 'print_directory',
    	bash_command = 'pwd',
    	)
    t2 = PythonOperator(
    	task_id = 'print_date',
    	python_callable=print_context,
    	)

    t1 >> t2
           
        
