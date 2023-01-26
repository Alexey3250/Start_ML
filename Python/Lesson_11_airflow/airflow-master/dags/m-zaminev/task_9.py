from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
    'm-zaminev_task_9',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='m-zaminev_task_9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 16),
    catchup=False,
    tags=['m-zaminev_task_9']

) as dag:

    def push_xcom(ti):
            ti.xcom_push(
                key='sample_xcom_key',
                value='xcom test'
            )
    def pull_xcom(ti):
            res = ti.xcom_pull(
                key='sample_xcom_key',
                task_ids='The_first_task'
            )
            print(res)

    t1 = PythonOperator(
            task_id='The_first_task',  
            python_callable=push_xcom)
    t2 = PythonOperator(
            task_id='The_second_task',  
            python_callable=pull_xcom,
        )
    t1 >> t2