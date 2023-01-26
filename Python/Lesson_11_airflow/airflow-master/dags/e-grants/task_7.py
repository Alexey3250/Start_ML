from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


with DAG (
    'task_7_e-grants',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='DAG for task_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 22),
    catchup=False,
    tags=['task_7'],
) as dag:
    
    def get_task_number(ts, run_id, **kwargs):
        print(f'task number is: {kwargs}')
        print(ts)
        print(run_id)    
    
    for n in range(10,30):
        task = PythonOperator(task_id = f"print_{n}_task_7", python_callable=get_task_number, op_kwargs={'task_number': n})
    