from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'hw_4_n-anufriev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},
        description='anufriev_lesson4',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 29),
        catchup=False,
        tags=['hw_4_n-anufriev']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'task_number_is_{i}',
            bash_command=f'print({i})')
    t1.doc_md = dedent(
        """\
    #### Bash Task
    `task_bash` *executes* a `bash_command=f"echo {i}"` which prints **task number** for `BashOperator`
    """
    )

    def print_task_number(task_number):
        print(f"task number is: {task_number}")


    for j in range(20):
        t2 = PythonOperator(
            task_id=f'print_task_num_{j}',
            python_callable=print_task_number,
            op_kwargs={'task_number': j})
    t2.doc_md = dedent(
        """\
    #### Bash Task
    `task_bash` *executes* a `bash_command=f"echo {i}"` which prints **task number** for `BashOperator`
    """
    )
    t1 >> t2
