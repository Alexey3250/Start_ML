from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_8_a-savelev-12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2022, 9, 15),
    tags=['a-savelev-12'],
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    def task_num(ts, run_id, **kwargs):
        task_number = kwargs['task_number']
        print(ts, run_id, task_number)
        return f'task number is: {str(number)}'
    
    for i in range(30):
        task = PythonOperator(
            task_id = f'a_savelev_12_task_{str(i)}',
            python_callable = task_num,
            op_kwargs= {'task_number' : str(i)}         
        )
        task 
    task.doc_md = dedent(
        """\
        # **Task** _Documentation_
        some `code=    
        """
    )