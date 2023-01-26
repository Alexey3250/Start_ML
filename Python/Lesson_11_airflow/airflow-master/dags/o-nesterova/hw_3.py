from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
	'hw_3_o-nesterova',
	default_args={
		'depends_on_past': False,
		'email': ['airflow@example.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
	},
	description='Olya',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2022, 12, 25),
	catchup=False,
	tags=['Olya'],
) as dag:
	for i in range(10): 
		t1 = BashOperator(
			task_id='task_' + str(i),
			bash_command=f"echo{i}",
		)
	
	def cnum(task_number):
		print('task number is:', task_number)
	
	for i in range(20):	
		t2 = PythonOperator(
			task_id='task_' + str(i+10),
			python_callable=cnum,
			op_kwargs={'task_number':float(i)+10},
		)

	t1>>t2