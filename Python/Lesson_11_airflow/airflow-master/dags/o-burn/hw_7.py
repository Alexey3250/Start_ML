'''
Добавьте в PythonOperator из второго задания (где создавали 30 операторов в цикле) kwargs и
передайте в этот kwargs task_number со значением переменной цикла.
Также добавьте прием аргумента ts и run_id в функции, указанной в PythonOperator, и
распечатайте эти значения.
'''

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_11_1_allburn',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='HW_1_burn',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['o-burn example'],
) as dag:

    for i in range(10):
        task_bash = BashOperator(
            task_id='show' + str(i),
            bash_command=f'echo {i}'
        )


    def print_task_number(task_number, ts, run_id, **kwargs):
        print(f"task number is: {task_number}")
        print(f"ts is: {ts}")
        print(f"run_id: {run_id}")
        print(kwargs)



    for i in range(10, 30):
        task_py = PythonOperator(
            task_id='show' + str(i),
            python_callable=print_task_number,
            op_kwargs = {'task_number' : i,
                         'ts' : "{{ ts }}",
                         'run_id': "{{ run_id }}"
                         }
        )

    task_bash >> task_py
