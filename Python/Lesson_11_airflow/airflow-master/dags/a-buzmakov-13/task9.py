from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

def push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
        )

def pull(ti):
    xcom_test = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='push_xcom'
        )
    print(xcom_test)

with DAG(
    'a-buzmakov-13_task_9',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='a-buzmakov-13_DAG_task9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 20),
    catchup=False,
    tags=['task_9'],
) as dag:
    
    a1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push
    )
    a2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull
    )
    a1>>a2
