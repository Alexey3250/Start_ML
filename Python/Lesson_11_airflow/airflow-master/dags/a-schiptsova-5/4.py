from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

def print_task(task_number):
	print(f'task number is: {task_number}')
	

with DAG(
    'a-schiptsova-5-hw-3',
    default_args = {
        'depends_on_past': False,
		'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes = 5)},
    description = 'DAG 3',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2022, 1, 1),
    catchup = False,
    tags = ['hw-3'],
) as dag:

	print_template = dedent(
        """
    {% for i in range(6) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """)

	t = BashOperator(
		task_id = 'Jinja_example',
		bash_command = print_template,
		dag = dag)