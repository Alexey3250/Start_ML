from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_4',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Lesson 11, homework 4',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['i_boztayev']

) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'bash_command{i}',
            bash_command=f'echo{i}' # какую bash команду выполнить в этом таске
        )

    t1.doc_md = dedent(
        """
        ###Abzac 
        
        *I do not understand what is going on here. But I try to figure it out.*
        
        **For example code**: 
        '''     
        for i in range(10):
        t1 = BashOperator(
            task_id=f'bash_command{i}',
            bash_command=f'echo{i}' # какую bash команду выполнить в этом таске
        )
        '''  
        
        """
    )

    def print_task_number(task_number):
        print(f"task number is: {task_number}")
        return "task number is printed"

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )


    # А вот так в Airflow указывается последовательность задач
    t1 >> t2
