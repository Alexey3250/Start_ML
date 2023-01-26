from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def push_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def pull_xcom(ti):
    xcom_sample = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='task_push',
    )
    print(xcom_sample)


with DAG(
    'HW_8_e-dracheva',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
        },
    
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['HW_8_e-dracheva'],
    ) as dag:
                   
    t1 = PythonOperator(
        task_id="xcom_push",
        python_callable=push_xcom)
    
    t2 = PythonOperator(
        task_id="xcom_pull",
        python_callable=pull_xcom)
    
    t1 >> t2    