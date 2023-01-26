from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

dag = DAG(
    'hw_2_utrobina',
    catchup=False,
    default_args=default_args,
    schedule_interval='30 15 * * *',
    start_date=datetime(2023, 1, 21),
    description='etl',
    tags=['j-utrobina']
)

def print_task_number(task_number):
    print(f"task number is: {task_number}")

# Генерируем таски в цикле - так тоже можно
for i in range(30):
    if i < 10:
        task = BashOperator(
            task_id = 'echo_task_' + str(i+1),
            bash_command=f"echo {i+1}",
            dag=dag
        )
    else:
        task = PythonOperator(
        task_id='print_task_number_' + str(i+1),  # в id можно делать все, что разрешают строки в python
        dag=dag,
        python_callable=print_task_number,
        op_kwargs={'task_number': i+1}
    )
    
    task
dag.doc_md = dedent("""
# Documentation
`PythonOperator` and `BashOperator` printed *task number* and **bla-bla-bla**
""")