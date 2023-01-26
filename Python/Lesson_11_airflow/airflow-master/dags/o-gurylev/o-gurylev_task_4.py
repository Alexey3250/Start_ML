
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent


def print_i(task_number, **kwargs):
    return print(f"task number is: {task_number}")


with DAG(
        'o-gurylev_task_4',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='o-gurylev_task_4',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    for i in range(10):
        m1 = BashOperator(
            task_id=f'bash_command{i}',
            bash_command=f"echo {i}")
        m1.doc_md = dedent(
            """\
        #### Bash Task
        `task_bash` *executes* a `bash_command=f"echo {i}"` which prints **task number** for `BashOperator`
        """
        )
    for task_number in range(20):
        m2 = PythonOperator(
            task_id=f'num_of_tasks_on_python{task_number}',
            op_kwargs={"task number": task_number},
            python_callable=print_i
        )
        m2.doc_md = dedent(
            """\
        #### Python Task
        `task_python` *call* task function `def task(task_number):` 
        which prints `task number is: {task_number}` for `PythonOperator`
        """
        )