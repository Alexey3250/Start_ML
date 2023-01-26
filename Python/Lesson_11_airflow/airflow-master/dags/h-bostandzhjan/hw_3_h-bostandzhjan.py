"""
### a simple tutorial dag for printing
"""
from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depend_on_pas': False,
    'email':['airflow@exampl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}
with DAG(
        'hw_2_h-bostandzhjan',
        default_args=default_args,
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021,1,1),
        catchup=False,
        tags=['hristo']
        ) as dag:

    for i in range(10):
        t1= BashOperator(
            task_id='t_1_echo_the'+str(i),
            bash_command=f'echo {i}')
        t1.dic_mc = dedent(
            """
            ### BashOperator
            В **task_1** распечатаеться подрят *10* чисел
            при помощи команды 'echo {i}'
            """
            )
    def print_task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        t2 = PythonOperator(
            task_id='t_2_python_'+str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number':i}
        )
        t2.doc_md=dedent(
            """
            ### PythonOperator
            В **таске_2** распечатывает подряд *20* чисел
            при помощи функции 'print_task_number',
            которая задействует переменную из цыкла.
            """
        )
    dag.dog_mc = __doc__
    t1 >> t2