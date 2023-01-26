from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'bragin_ex_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = 'A simple bash DAG ex_2',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 9, 9),
    catchup = False,
    tags=['example'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'echo_{i}',
            bash_command=f"echo {i}",
        )


    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'


    for i in range(20):
        run_this = PythonOperator(
            task_id='task_' + str(i),
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )