from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task_number(task_number):
    print(f"task number is: {task_number}")
    return "task number printed"


with DAG(
    # название
'hw_3_d-otkaljuk',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
description='Py and Bush operation',
schedule_interval=timedelta(days=1),
start_date=datetime(2022, 10, 29),
catchup=False,
tags=['hw_3_d_otkaljuk'],
) as dag:
    for i in range(30):
        if i < 10:
            t_1 = BashOperator(
                task_id=f"echo_task_number{i}",
                bash_command=f"echo {i}"
            )
        else:
            t_2 = PythonOperator(
                task_id='print_task_number_' + str(i),
                python_callable=print_task_number,
                op_kwargs={'task_number':i},
            )

    t_1 >> t_2from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task(**kwargs):
    return print(f"task number is: {kwargs.get('task_number')}")

with DAG(
    # название
'hw_3_d-otkaljuk',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
description='Py and Bush operation',
schedule_interval=timedelta(days=1),
start_date=datetime(2022, 10, 29),
catchup=False,
tags=['hw_3_d_otkaljuk'],
) as dag:
    for i in range(30):
        if i < 10:
            t_1 = BashOperator(
                task_id=f"echo_task_number is: {i}",
                bash_command=f"echo {i}"
            )
            else:
            t_2 = PythonOperator(
                task_id='print_task_number is: ' + str(i),
                python_callable=print_task_number,
                op_kwargs={'task_number': i},
            )

    t_1 >> t_2