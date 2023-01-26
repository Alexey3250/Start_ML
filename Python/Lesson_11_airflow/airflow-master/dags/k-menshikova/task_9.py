from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


with DAG(
    'hw_9_k-menshikova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  
    },
    description='hw_9_k-menshikova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_9_k-menshikova'],
) as dag:

    def push_xcom(ti):
        return "Airflow tracks everything"

    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom,
    )


    def retrieve_xcom(ti):
        
        print(ti.xcom_pull(key="return_value", task_ids="push_xcom"))

    t2 = PythonOperator(
        task_id='retrieve_xcom',
        python_callable=retrieve_xcom,
    )

    t1 >> t2
