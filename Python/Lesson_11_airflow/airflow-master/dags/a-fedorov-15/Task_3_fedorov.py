
from datetime import datetime, timedelta

from textwrap import dedent


from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.operators.bash import BashOperator

with DAG(

    'Task_3_fedorov',

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

    start_date=datetime(2022, 1, 1),

    catchup=False,

    tags=['example'],

) as dag:

    for i in range(10):
    
        t1 = BashOperator(

            task_id = "echo" + str(i),  
        
            bash_command = f"echo {i}",  

        )


    def print_task(task_number):

        print(f"task number is: {task_number}")

    
    for i in range(20):
    
        Number_task = PythonOperator(

            task_id="print_Number_task" + str(i),  

            python_callable = print_task,

            op_kwargs={"task_number": i}

        )
    

    t1 >> Number_task