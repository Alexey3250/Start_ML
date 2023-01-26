from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'dm-kuznetsov_hw_4',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='dm-kuznetsov_hw_4',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 3),
        catchup=False,
        tags=['task_1'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f'echo_{i}',
            bash_command=f'echo {i}',
        )
        t1.doc_md = dedent(
            """\
        # Task Documentation
        **10** *echos* `f'echo {i}'` and etc   
           """
        )

    def task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id = f'task_number_{i}',
            python_callable=task_number,
            op_kwargs={'task_number': i}
        )
        t2.doc_md = dedent(
            """\
        # Task Documentation
        **20** *numbers* `"task number is: {task_number}"` and etc  
           """
        )

t1 >> t2