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
    task_number = kwargs['task_number']
    print(f"task number is: {task_number}")
    return 'Whatever you return gets printed in the logs'


with DAG(
        '2_a-beljaninov-14',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=5),
        default_args=default_args,
        catchup=False
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print_date_' + str(i),
            bash_command=f"echo {i}",
        )
    for i in range(20):
        t2 = PythonOperator(
            task_id='print_the_context_' + str(i),
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )

