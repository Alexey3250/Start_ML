from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'm-zaminev_task_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="m-zaminev_task_3",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 16),
        catchup=False,
        tags=['m-zaminev_task_3'],
) as dag:
    for i in range(10):
        task1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f"echo {i}"
        )

        task1.doc_md = dedent(
            """\
            10 задач типа BashOperator  
            """
        )


    def print_task_number(task_number):
        print(f'task number is -  {task_number}')


    for i in range(20):
        task2 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )
        task2.doc_md = dedent(
            """\
            20 задач PythonOperator  
            """
        )

task1 >> task2