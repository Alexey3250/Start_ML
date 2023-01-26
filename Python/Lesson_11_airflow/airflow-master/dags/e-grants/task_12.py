from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable



with DAG (
    'task_12_e-grants',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='DAG for task_12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 22),
    catchup=False,
    tags=['task_12'],
) as dag:
    
    def get_variable():
        is_startml = Variable.get("is_startml")
        print(is_startml)
        
    task = PythonOperator(task_id = "print_task_12", python_callable=get_variable)
    
    task
    