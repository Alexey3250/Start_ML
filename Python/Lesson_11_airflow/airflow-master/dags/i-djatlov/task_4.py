from textwrap import dedent
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_3_i-djatlov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='task 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 17),
    catchup=False,
    tags=['example']
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='echo'+str(i),
            bash_command=f"echo {i}",
    ) 
        t1.doc_md = """\
            #Documentation
            **t1** makes echo *ten* times
            contains function `echo {i}`
        """
    def print_funcnum(task_number):
        print('task number is:', task_number)
        
    for i in range(20):
        t2 = PythonOperator(
            task_id='funcmnum'+str(i),
            python_callable=print_funcnum,
            op_kwargs={'task_number': i},
        )
        t2.doc_md = """\
            **t2** makes print func *twenty* times
            **t2** contains function `print_funcnum` and print func number
        """
    
    t1 >> t2


    