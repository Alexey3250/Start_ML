from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def print_ds(ds):
    print(ds)

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'DAG_HW_2_ponomareva',
    default_args=default_args,
    description='tasks for HW_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 10),
    catchup=False,
    tags=['ponomareva']
) as dag:
    t1 = BashOperator(
        task_id='HW_2_Bash',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='HW_2_Python',
        python_callable=print_ds,
    )

    t1 >> t2