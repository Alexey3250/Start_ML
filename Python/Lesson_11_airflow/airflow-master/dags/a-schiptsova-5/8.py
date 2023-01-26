from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

def push_xcom(ti):
	ti.xcom_push(key = 'sample_xcom_key', value = 'xcom test')
	
def pull_xcom(ti):
	print(ti.xcom_pull(key = 'sample_xcom_key', task_ids = 'push'))
	

with DAG(
    'a-schiptsova-5-hw-8',
    default_args = {
        'depends_on_past': False,
		'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes = 5)},
    description = 'DAG 8',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2022, 1, 1),
    catchup = False,
    tags = ['hw-8'],
) as dag:

	t1 = PythonOperator(
		task_id = 'push',
		python_callable = push_xcom)
		
	t2 = PythonOperator(
		task_id = 'pull',
		python_callable = pull_xcom)
		
	t1 >> t2