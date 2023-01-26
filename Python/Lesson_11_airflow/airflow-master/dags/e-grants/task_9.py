from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


def get_xcom_push(ti):
    ti.xcom_push(key='sample_xcom_key',value='xcom test')
    # print("Done")
        
def get_xcom_pull(ti):
    ti.xcom_pull(key='sample_xcom_key',task_ids='display_task_9_push')
    # print("Done")


with DAG (
    'task_9_e-grants',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='DAG for task_9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 22),
    catchup=False,
    tags=['task_9'],
) as dag:
    
    task_1 = PythonOperator(task_id = "display_task_9_push", python_callable=get_xcom_push)
    task_2 = PythonOperator(task_id = "display_task_9_pull", python_callable=get_xcom_pull)
    
    task_1 >> task_2