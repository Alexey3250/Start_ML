from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_7_a-shapovalov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Excercise 3 a-shapovalov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_7_a-shapovalov']
) as dag:
    for i in range(1, 11):
        bash = BashOperator(
            task_id=f'bash_command_{i}',
            bash_command=f'echo {i}')

    def print_task_number(ts, run_id, **kwargs):
        print(ts)
        print(run_id)


    for i in range(11, 31):
        python = PythonOperator(
            task_id=f'python_command_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

    bash >> python
