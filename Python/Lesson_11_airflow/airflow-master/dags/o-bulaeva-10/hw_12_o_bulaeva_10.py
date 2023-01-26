from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable


def get_variable():
	is_startml = Variable.get("is_startml")
	print(is_startml)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_9_o_bulaeva_10',
    start_date=datetime(2022, 7, 29),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id = 'get_variable',
        python_callable=get_variable
    )
