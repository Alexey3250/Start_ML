from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


with DAG (
    'task_2_e-grants',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='DAG for task_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 20),
    catchup=False,
    tags=['task_2'],
) as dag:
    task_1 = BashOperator(task_id="run_pwd_command", bash_command="pwd")
    
    
    def print_ds(ds):
        print(ds)
        print("Printed out")
        return 'All is ok'
    
    
    task_2 = PythonOperator(task_id = "print_ds", python_callable=print_ds)
    
    
    task_1 >> task_2