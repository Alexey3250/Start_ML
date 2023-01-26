from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
        's-duzhak-2-task_6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 11),
        catchup=False,
        tags=['example'],
) as dag:
    def print_task_num(task_number):
        print(f"task number is: {task_number}")

    task1 = BashOperator(
        task_id='bash_1',
        bash_command='echo 1',
        env={'NUMBER': 1},
        dag=dag
    )
    for i in range(2, 11):
        task = BashOperator(
            task_id=f'bash_{i}',
            bash_command=f'echo $NUMBER',
            env={'NUMBER': i}
        )
        task.doc_md = dedent("""
        # Header
        *Курсив*
        **Жирный**
        `code`
        """)
        task1 >> task

    for i in range(11, 31):
        task = PythonOperator(
            task_id=f'python_{i}',
            python_callable=print_task_num,
            op_kwargs={'task_number': i},
        )
        task.doc_md = dedent("""
             # Header
             *Курсив*
             **Жирный**
             `code`
             """)
        task1 >> task

