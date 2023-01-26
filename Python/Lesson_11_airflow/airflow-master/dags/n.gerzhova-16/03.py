from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'third',
        default_args={
            'depends_on_past': False,
            'email': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='DAG for Task 02',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 23),
) as dag:

    for i in range(10):
        t_0_10 = BashOperator(
            task_id='print_smth_' + str(i),
            bash_command=f"echo {i}",
        )

    def print_task_N(task_number):
        print("task number is: {task_number}")

    for i in range(11, 31, 1):
        t_11_20 = PythonOperator(
            task_id='task_' + str(i),
            python_callable=print_task_N,
            op_kwargs={'task_number': i},
        )

    dag.doc_md = dedent(
    """
    # DAG for 03
    ***this is the documentation for 03***
    
    ### 2 tasks
    
    **Task 1**
    #
    __Task 2__
    _create function_

    `some function`

    """
    )

    t_0_10 >> t_11_20
