from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def push_value(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test',
    )
    
def pull_value(ti):
    value = ti.xcom_pull(
                key='sample_xcom_key',
                task_ids='push_value',
            )
    print(value)

with DAG(
    'HW_9_a.platov',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),         
        },
        description='Home Work N9 (XCom)',
        start_date=datetime(2022, 7, 24),
        catchup=False,
        tags=['a.platov'],
    ) as dag:
           
        opr_push_data = PythonOperator(
                task_id = "push_value",
                python_callable=push_value,
                )
        opr_pull_data = PythonOperator(
                task_id="pull",
                python_callable=pull_value,
                )
        
        opr_push_data >> opr_pull_data
