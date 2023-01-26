from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def test_xcom_push(ti):
    ti.xcom_push(
        key = 'sample_xcom_key',
        value = 'xcom test'
    )

def test_xcom_pull(ti):
    res = ti.xcom_pull(
        key = 'sample_xcom_key',
        task_ids = 'task_push'
    )
    print(res)

with DAG(
	'hw_9',
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
	t1 = PythonOperator(
		task_id='task_push',
		python_callable=test_xcom_push,
	)
	t2 = PythonOperator(
		task_id='task_pull',
		python_callable=test_xcom_pull,
	)
	t1 >> t2
