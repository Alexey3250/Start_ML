from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

def task_number_printer(task_number):
    print(f"task number is: {task_number}") 

with DAG('hw_4_o_bulaeva_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries':1,
            'retry_delay': timedelta(minutes=5),
            },
        description = 'Second DAG',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2022, 7, 24),
        catchup = False) as dag:

    for i in range(10):
    	bash_task = BashOperator(
    	task_id = "bash_task_number_" + str(i),
    	bash_command = f"echo {i}"
    	)
    bash_task.doc_md = dedent(
        """\
    #### Bash Task Documentation
    *Execute* **bash** command `echo`
    """
    )

    for i in range(20):
        python_task = PythonOperator(
        task_id='python_task_number_' + str(i),  
        python_callable=task_number_printer,
        op_kwargs={'task_number': i}
        )
    python_task.doc_md = dedent(
        """\
    #### Python Task Documentation
    *Print* task number with **python function** `print("")`
    """
    )
    bash_task << python_task
