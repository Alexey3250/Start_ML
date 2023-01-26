from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_10_p-tarabanov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    
    description='10 task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 9, 11),
    catchup=False,
    tags=['XCom return'],
) as dag:
    
    def ret_string():
        return "Airflow tracks everything"
    
    task1 = PythonOperator(
        task_id = 'task_return_string',
        python_callable = ret_string
    )
    
    def get_string(ti):
        xcom_value = ti.xcom_pull(
            key='return_value',
            task_ids='task_return_string'
        )
        print(f'xcom value from task_return_string {xcom_value}')
    
    task2 = PythonOperator(
        task_id = 'task_get_string',
        python_callable = get_string    
    )
    
    task1 >> task2