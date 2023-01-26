from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
		'depends_on_past': False,
		'email': ['airflow@example.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delay': timedelta(minutes=5),
}

def print_ds(ds):
	print(ds)

with DAG(
	'first_dag_rag',
	default_args=default_args,
	description='task_2_first_DAG',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2022, 1, 1),
	catchup=False,
	tags=['rag'],
) as dag:

	t1 = BashOperator(
		task_id='print_actual_directory_with_my_code',
		bash_command='pwd'
		)

	t2 = PythonOperator(
		task_id='print_current_logical_date',
		python_callable=print_ds
		)

	t1 >> t2