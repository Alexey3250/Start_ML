from datetime import datetime, timedelta
from textwrap import dedent
# To declare a DAG the DAG class of the airflow must be imported
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    "m-chajkovskij_11.7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes =5),
    },
    description='task_11.7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['homework']
)as dag:

    def print_number(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(f"ts: {ts}")
        print(f"run_id: {run_id}")

    for i in range(20):
        task = PythonOperator(task_id=f'python_task_{i}', python_callable=print_number, op_kwargs={'task_number': i})
        task.doc_md = """
        ## Python task documentation in the form of *markdown*. 
        The **task** is created using `PythonOperator` from the module *airflow*
        """
