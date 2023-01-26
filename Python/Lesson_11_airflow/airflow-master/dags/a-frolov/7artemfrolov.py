from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw7_afrolov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
    ,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['afrolov']
) as dag:

    for i in range(10):
        task = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f'echo{i}'
        )
        task

    def my_func(ts, run_id, **op_kwargs):
        tn = op_kwargs['task_number']
        print(f'task number is {i}')
        print(ts)
        print(run_id)

    for i in range(10,30):
        task = PythonOperator(
            task_id = 'print_' + str(i),
            python_callable=my_func,
            op_kwargs={'task_number': i},
        )
        task