"""
 Second dag. The program executes 10 bash commands (print 1,2,3, ..., 10) 
 and 20 python prints (print tasks number)
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

from datetime import datetime, timedelta

with DAG(
    'hm_4_e-poltorakova',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    },
   
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['e-poltorakova'],

) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='bash_print_' + str(i),  
            bash_command=f'echo {i}',  
        )

    def print_number_task(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        t2 = PythonOperator(
            task_id='python_print_' + str(i),
            python_callable=print_number_task,
            # передаем в аргумент 
            op_kwargs={'task_number': i},
        )
    
    t1.doc_md = dedent(
        """
        #### Task Documentation
        There are **10** tasks in the cycle. 
        #Each command executes `bash` - `echo`

        *11 lesson ML*
        """
    )

    t1.doc_md = dedent(
        """
        #### Task Documentation
        There are **20** tasks in the cycle. 
        #Each command executes `python operator`. 
        #Each task print the task number.

        *11 lesson ML*
        """
    )
    
    t1 >> t2
