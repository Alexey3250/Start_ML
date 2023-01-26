from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'tutorial',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='First task',
        start_date=datetime(2022, 7, 17),
        catchup=False,
        tags=['first_task'],
    ) as dag:
        def print_msg(task_number):
            print(f"task number is: {task_number}")
            
        for task_number in range(1, 31):
            if task_number <= 10:
                t_bash = BashOperator(
                    task_id=f'HW_3_'+str(task_number),  
                    bash_command=f'echo {task_number}', 
                    dag=dag,
                )
            else:
                task_p = PythonOperator(
                    task_id='HW_3_'+str(task_number),  
                    python_callable=print_msg,
                    op_kwargs={task_number},
                )
        
t_bash >> task_p