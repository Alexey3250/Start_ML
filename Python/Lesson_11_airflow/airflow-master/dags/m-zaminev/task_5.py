from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'm-zaminev_task_5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="m-zaminev_task_5",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 16),
        catchup=False,
        tags=['m-zaminev_task_5'],
) as dag:
    def print_task_number(ts, run_id, task_number):
        print(f"task number is: {task_number}")
        print(f'ts:{ts}')
        print(f'run_id:{run_id}')


    for i in range(5):
        task = PythonOperator(
            task_id=f'print_arguments_{i}',
            op_kwargs={'task_number': i},
            python_callable=print_task_number
        )

    task
