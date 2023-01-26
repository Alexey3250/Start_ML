from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_9_p-tarabanov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    
    description='9 task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 9, 11),
    catchup=False,
    tags=['XCom'],
) as dag:
    
    def push_var_xcom(ti):
        
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'      
        )
        
        
    def pull_var_xcom(ti):
        
        xcom_value = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='task_push_var_xcom'
        )
        print(f'xcom_value - {xcom_value}')
    
    task1 = PythonOperator(
        task_id = 'task_push_var_xcom',
        python_callable=push_var_xcom,
    )
    
    task2 = PythonOperator(
        task_id = 'task_pull_var_xcom',
        python_callable=pull_var_xcom,
    )
    task1 >> task2