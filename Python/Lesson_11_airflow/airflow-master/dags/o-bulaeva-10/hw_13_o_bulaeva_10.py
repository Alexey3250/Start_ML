from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable


def find_next_step():
	is_startml = Variable.get("is_startml")
	if is_startml == "True":
		task_id = "startml_desc"
	else:
		task_id = "not_startml_desc"
	return task_id

def startml_desc():
	print("StartML is a starter course for ambitious people")

def not_startml_desc():
	print("Not a startML course, sorry")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_13_o_bulaeva_10',
    start_date=datetime(2022, 7, 29),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    start = DummyOperator(task_id = 'before_branching')

    t0 = BranchPythonOperator(
    	task_id = 'find_next_step',
        python_callable=find_next_step)

    t1 = PythonOperator(
        task_id = 'startml_desc',
        python_callable=startml_desc
    )

    t2 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable=not_startml_desc
    )

    end = DummyOperator(task_id = 'after_branching')

    start >> t0 >> [t1, t2] >> end