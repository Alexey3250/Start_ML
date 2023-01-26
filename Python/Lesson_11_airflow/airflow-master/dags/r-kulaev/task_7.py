from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_7_r-kulaev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_7_r-kulaev',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 27),
        catchup=False,
        tags=['hw_7_r-kulaev']
) as dag:
    def print_task_number(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)


    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f'print_task_{i}',
                bash_command=f'echo {i}'
            )
        else:
            t2 = PythonOperator(
                task_id=f'task_number_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )

    t1 >> t2
