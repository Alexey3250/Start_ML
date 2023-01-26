from datetime import datetime, timedelta

from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

"""
Test documentation
"""

def print_text(task_number, ts, run_id, **kwargs):
    print(f'task number is: {task_number}')
    print(ts)
    print(run_id)
    print(kwargs)
    return "done"
    
    

with DAG(
    'hw7_bm',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw7_bm DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['BH_hw7'],
) as dag:
    
    t1 = DummyOperator(task_id='start_dag')
    t2 = DummyOperator(task_id='wait_for_all_bash_operators')
    t3 = DummyOperator(task_id="finish_dag")
    
    for i in range(10):
        bash_task = BashOperator(
            task_id = f'print_echo_{i}',
            bash_command = 'echo $NUMBER',
            env = {'NUMBER': str(i)}
        )
        
        t1 >> bash_task >> t2
        
        bash_task.doc_md = dedent(
            f"""\
            #### Task {i} documentation
            Используя `Python`, можно достичь **больших** высот в _программировании_ :)))
            """
        )
        
    for i in range(20):
        python_task = PythonOperator(
            task_id = f'print_text_{i}',
            python_callable = print_text,
            op_kwargs={"task_number": i}
        )
        
        t2 >> python_task >> t3
   