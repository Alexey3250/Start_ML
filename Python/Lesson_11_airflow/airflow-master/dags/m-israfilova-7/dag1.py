
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

doc_md = 
"""
#Абзац
`t1 = BashOperator(
        task_id='print_pwd', 
        bash_command='pwd'  
    )

    def print_ds(ds, **kwargs):
        print(ds)
        return ds
        
    t2 = PythonOperator(
        task_id='print_ds',  
        python_callable=print_ds
    ) `
*cursive text*
**bold* text*
"""

with DAG(
    'myseconddag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    }, 
    description='Just for practice',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=[ 'masha' ],
) as dag:

    

    for i in range(10):
        bashtask = BashOperator(
            task_id='bashtask' + str(i),  
            bash_command = f"echo {i}",
        )

    def numberofthetask(task_number):
        print (f'task number is: {task_number}')

    for i in range(10, 30):
        task = PythonOperator(
            task_id='task' + str(i),  
            python_callable=numberofthetask,
            op_kwargs={'task_number': int(i)},
        )

# t1 >> t2 

