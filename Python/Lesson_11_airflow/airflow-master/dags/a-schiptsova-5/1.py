from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_arg(ds):
	print(ds)

with DAG(
    'a-schiptsova-5-hw-1',
    default_args = {
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes = 1)},
    description = 'DAG 1',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2022, 1, 1),
    catchup = False,
    tags = ['hw-1'],
) as dag:

	t1 = BashOperator(
        task_id = 'print_dir',
        bash_command = 'pwd',
		dag = dag)
		
	t2 = PythonOperator(
        task_id = 'print_arg',
        python_callable=print_arg)
		
	t1 << t2