from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator



with DAG (
    'task_6_e-grants',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='DAG for task_6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 22),
    catchup=False,
    tags=['task_6'],
) as dag:
    
    for i in range(10):
        task = BashOperator(
            task_id=f"print_NUMBER_{i}_task_6", 
            bash_command=f"echo $NUMBER", 
            env = {"NUMBER": i}
            )
        
   
