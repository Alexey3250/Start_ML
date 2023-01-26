from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'mythirddag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    }, 
    description='Just for practice',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=[ 'masha' ],
) as dag:
	for i in range(5):
		bashtask = BashOperator(
        task_id='loopbash' + str(i),  
        bash_command = "echo {{ts}} {{run_id}} ",
    )