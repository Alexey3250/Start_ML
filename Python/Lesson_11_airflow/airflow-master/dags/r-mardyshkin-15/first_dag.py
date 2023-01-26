from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
	'hw_2',
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
	t1 = BashOperator(
		task_id='my_path',
		bash_command='pwd',
	)
	
	def know_date(ds, **kwargs):
		print(ds)
	
	t2 = PythonOperator(
		task_id='my_date',
		python_callable=know_date,
	)

	t1 >> t2
