from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
	'hw_7',
	default_args={
		'depends_on_past': False,
		'email': ['airflow@example.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delay': timedelta(minutes=5)
	},  
    start_date = datetime(2022, 12, 26)
) as dag:

    def task_number_print(ts, run_id, **kwargs):
        print(kwargs)
        print(ts)
        print(run_id)

    for i in range(30):
        if i < 10:
                task = BashOperator(
                    task_id = 'task_' + str(i + 1),
                    bash_command = f'echo {i + 1}'
                )
        else:
                task = PythonOperator(
                    task_id = 'task_' + str(i + 1),
                    python_callable = task_number_print,
                    op_kwargs = {'task_number' : i}
                )
                