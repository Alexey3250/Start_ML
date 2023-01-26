from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


def get_string():
    return "Airflow tracks everything"

def get_xcom_pull(ti):
    ti.xcom_pull(key="return_value", task_ids="display_task_10")


with DAG (
    'task_10_e-grants',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='DAG for task_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 22),
    catchup=False,
    tags=['task_10'],
) as dag:
    
    task_1 = PythonOperator(task_id = "display_task_10", python_callable=get_string)
    task_2 = PythonOperator(task_id = "display_task_10_pull", python_callable=get_xcom_pull)
    
    task_1 >> task_2