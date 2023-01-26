from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_(ds):
    print(ds)
    print('DAG date')


with DAG(
    'n-tugushev-10-task2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Fist DAG Ml Karpov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 17),
    catchup=False,
    tags=['example', 'n-tugushev-10'],
) as dag:
    t1 = BashOperator(
        task_id='print_airflow_execute_directory',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print',
        python_callable=print_,
    )

t1 >> t2
