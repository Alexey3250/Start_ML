from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'HW_7_a.platov',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Home Work N7',
        start_date=datetime(2022, 7, 24),
        catchup=False,
        tags=['a.platov'],
    ) as dag:
        
        date = "{{ ds }}"

        def print_date(task_number, ts, run_id):
            print(ts)
            print(run_id)
            print('task number: ', task_number)
    
        def task_bash(task_number: int, date):
            return  BashOperator(
                    task_id='run_bush_op_'+str(task_number),                 
                    bash_command='echo $NUMBER',
                    env={"NUMBER": str(task_number), "DATA START": date},
                    dag=dag,)

        def task_python(task_number: int, func):
            return PythonOperator(
                    task_id='run_python_op_'+str(task_number),
                    op_kwargs={"task_number": task_number,
                               'ts': "{{ ts }}",
                               'rub_id': "{{ run_id }}"},
                    python_callable=func,
                    )
        task = 0 
        for i in range(30):
            if i == 0:
                task = task_bash(i, date)
            elif i < 10:
                new_task = task_bash(i, date)
                task >> new_task
                task = new_task
            else:
                new_task = task_python(i, print_date)
                task >> new_task
                task = new_task
