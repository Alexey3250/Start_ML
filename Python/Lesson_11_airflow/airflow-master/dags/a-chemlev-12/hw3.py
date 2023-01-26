from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'chemelson_hw3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='chemelson_hw3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 11),
        catchup=False,
        tags=['chemelson_hw3']
) as dag:
    def print_task_number(num):
        print(f'task number is: {num}')


    for i in range(30):
        if i < 10:
            bash_task = BashOperator(
                task_id=f'print_task_{i}',
                bash_command=f'echo {i}'
            )
        else:
            python_task = PythonOperator(
                task_id=f'task_number_{i}',
                python_callable=print_task_number,
                op_kwargs={'num': i}
            )

    bash_task >> python_task


