from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def push_value(ti):
    return "Airflow tracks everything"
    
def pull_value(ti):
    value = ti.xcom_pull(
                key='return_value',
                task_ids='push_value',
            )
    print(value)

with DAG(
    'HW_10_a.platov',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),         
        },
        description='Home Work N10 (XCom with key = return_value)',
        start_date=datetime(2022, 7, 24),
        catchup=False,
        tags=['a.platov'],
    ) as dag:
           
        opr_push_data = PythonOperator(
                task_id = "push_value",
                python_callable=push_value,
                )
        opr_pull_data = PythonOperator(
                task_id="pull_value",
                python_callable=pull_value,
                )
        
        opr_push_data >> opr_pull_data
