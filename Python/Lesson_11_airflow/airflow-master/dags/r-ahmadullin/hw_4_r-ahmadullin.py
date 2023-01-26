from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'a.burlakov-9_task_3',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 

    },
    description='a.burlakov-9_DAG_task_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 3),
    catchup=False,
    tags=['task_3'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f'echo {i}',
        )
        t1.doc_md = dedent(
        """\
    # Task Documentation
    Первые **10 задач** типа *BashOperator*,  
    в них выполнена команда, использующая переменную цикла:  `"f"echo {i}"`   
       """
    )  # dedent - это особенность Airflow, в него нужно оборачивать всю доку

    def get_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id = 'task_number'+str(i),
            python_callable=get_task_number,
            op_kwargs={'task_number': i}
        )
        t2.doc_md = dedent(
        """\
    # Task Documentation
    **text** типа *text*,  
     `op_kwargs`   
    и `"task number is: {task_number}"`,
    где task_number - номер задания из цикла.
        """
    )
        
        
t1 >> t2