from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'Lesson_11_step_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='My second training DAG',
        start_date=datetime(2022, 6, 15),
        schedule_interval=timedelta(days=1),
        catchup=False,
        tags=['e.zabuzov', 'step_4']
) as dag:
    dag.doc_md = '''
    # Lesson 11 step 4 DAG
    this DAG uses cycle for making **10** *Bashoperators* and **20** *Pythonoperators*.
    the cycle model is:
    ```python
    for i in range(20):
        if i < 10:
            # making Bash commands tasks
        #making Python commands tasks
    ``` 
    '''


    def print_task_number(task_number, **kwargs):
        print(f'task number is: {task_number}')


    for i in range(20):
        if i < 10:
            t_bash = BashOperator(
                task_id=f'BashOperator_{i}',
                bash_command=f'echo {i}'
            )
        t_py = PythonOperator(
            task_id=f'PythonOperator_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )
    # t_bash >> t_py
