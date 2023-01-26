from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def python_operator():
	op_kwargs = 20
	print ("task number is: 3")

with DAG (
	'tutorial',
	default_args={
		'depends_on_past': False,
		'email': ['setto.box@gmail.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delivery': timedelta(minutes=5),
	},
	description='A simple tutorial DAG',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2022, 1, 1),
	catchup=False,
	tags=['example'],
) as dag:

	for i in range (10):

		t1 = BashOperator(
			task_id='bash_command_3',
			bash_command='f"echo {i}',
		)
	for i in range (op_kwargs):
		t2 = PythonOperator(
			task_id = 'pws_command',
			python_callable=python_operator,
		)
		t1>>t2