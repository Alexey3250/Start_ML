from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from textwrap import dedent


with DAG (
    'task_4_e-grants',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='DAG for task_4',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 22),
    catchup=False,
    tags=['task_4'],
) as dag:
    
    for i in range(10):
        task_1 = BashOperator(task_id=f"print_{str(i)}_task", bash_command = f"echo {i}")
    
    
    def get_task_number(task_number):
        print(f'task number is: {task_number}')
    
    
    for n in range(10,30):
        task_2 = PythonOperator(task_id = f"print_{str(n)}_tasks", python_callable=get_task_number, op_kwargs={'task_number': n})
        
        
    task_1.doc_md = """
        # Documentation for task_1
        This **task** should print _first_ 10 tasks. 
        I use **for** for it: 
        `for i in range(10)` 
        """
    
    
    task_2.doc_md = """
        # Documentation for task_2
        This **task** should print _rest_ 20 tasks. 
        I use **for** for it: 
        `for n in range(10,30)` 
        """
        
    task_1 >> task_2