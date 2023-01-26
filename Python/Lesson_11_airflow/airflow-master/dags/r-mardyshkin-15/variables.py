from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

def print_var():
    is_startml = Variable.get('is_startml')
    print (is_startml)


with DAG(
	'hw_12',
	default_args={
		'depends_on_past': False,
		'email': ['airflow@example.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delay': timedelta(minutes=5)
	},  
    start_date=datetime(2022, 12, 26)
) as dag:	
	task = PythonOperator(
		task_id='var',
		python_callable=print_var
	)
