from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta
from datetime import datetime

with DAG(
    'hw_2_j_ponomareva_01',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG_jp',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['j_pon_01'],
) as dag:

    t1 = BashOperator(
        task_id='print_date_users',
        bash_command='pwd',
    )

    def print_context(ds):
        print(ds)
        print('Грызи гранит науки')

    t2 = PythonOperator(
        task_id='print_the_context_user',
        python_callable=print_context,
    )


    t1 >> t2