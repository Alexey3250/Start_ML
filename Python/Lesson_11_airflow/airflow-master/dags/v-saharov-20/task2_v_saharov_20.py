from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
}

def print_time(ds):
    print(ds)

with DAG(
        dag_id="task2_v-saharov-20",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
)as dag:
    t1 = BashOperator(
        task_id="pwd_directory",
        bash_command="pwd"
    )
    t2 = PythonOperator(
        task_id="print_time",
        python_callable=print_time
    )

t1 >> t2