from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

with DAG(
    'hm_10_e-poltorakova',
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
    tags=['e-poltorakova'],

) as dag:

    def return_str():
        return "Airflow tracks everything"

    def puller(ti):
        pulled_value = ti.xcom_pull(key='return_value', task_ids='push')
        print(pulled_value)

    task1 = PythonOperator(
        task_id='push',
        python_callable=return_str,
    )
    task2 = PythonOperator(
        task_id='pull',
        python_callable=puller,
    )

    task1 >> task2