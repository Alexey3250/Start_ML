from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

def print_task(ts, run_id, **kwargs):
	x = kwargs['task_number']
	
	print(ts)
	print(run_id)
	print(f'task number is: {x}')
	

with DAG(
    'a-schiptsova-5-hw-6c',
    default_args = {
        'depends_on_past': False,
		'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes = 5)},
    description = 'DAG 6',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2022, 1, 1),
    catchup = False,
    tags = ['hw-6c'],
) as dag:

	t1 = DummyOperator(task_id = "start_point")
	t2 = DummyOperator(task_id = "mid_point")
	t3 = DummyOperator(task_id = "end_point")
	

	for i in range(30):
		if i < 10:
			t = BashOperator(
				task_id = 'task_' + str(i + 1),
				bash_command = f'echo {i}',
				dag = dag)
			t.doc_md = dedent(
				"""
				### **Task** {i} doc
				`echo` task number
				""")
			t2 << t << t1
			
		else:
			t = PythonOperator(
				task_id = 'task_' + str(i + 1),
				python_callable = print_task,
				op_kwargs={'task_number': i})
			t.doc_md = dedent(
				"""
				### Task {i} doc
				Print task number
				""")
			t3 << t << t2