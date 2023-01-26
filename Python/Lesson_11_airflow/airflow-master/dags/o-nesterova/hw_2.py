from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
	'hw_2_o-nesterova',
	default_args={
		'depends_on_past': False,
		'email': ['airflow@example.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
	},
	description='A simple tutorial DAG',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2022, 12, 25),
	catchup=False,
	tags=['Olya'],
) as dag:
	t1 = BashOperator(
		task_id='my_path',
		bash_command='pwd',
	)
	
	def know_date(ds, **kwargs):
		print('time = ', ds)
	
	t2 = PythonOperator(
		task_id='my_date',
		python_callable=know_date,
	)

	t1>>t2