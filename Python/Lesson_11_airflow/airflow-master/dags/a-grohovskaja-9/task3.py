from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

with DAG(
    'a-grohovskaja-9_hw_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a-grohovskaja-9_hw_3'],
) as dag:
    def print_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(30):
        if i<10:

            t1 = BashOperator(
                task_id='bash_part'+str(i),
                bash_command=f"echo {i}"
            )
        else:
            t2 = PythonOperator(
                task_id='python_part_'+str(i),
                python_callable=print_task_number,
                op_kwargs={'task_number':i}
            )

    t1.doc_md = dedent(
        """\
    # Bash Task Documentation
    Command `f"echo {i}"` use for printing *i*.
    """
    )

    t2.doc_md = dedent(
        """\
    # Python Task Documentation
    Python part only print **number** of task.
    """
    )

    t1 >> t2