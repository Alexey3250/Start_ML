from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def print_context(ds, **kwargs):
    print(ds)
    return 'Whatever you return gets printed in the logs'


with DAG(
        'hw_2_m-taljanova',
        start_date=datetime(2022, 12, 11),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=5),
        default_args=default_args,
        catchup=False
) as dag:
    t1 = BashOperator(
        task_id='print_date',
        bash_command='pwd',
    )
    t2 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
    )

    t1 >> t2
