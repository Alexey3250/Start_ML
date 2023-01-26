from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
    'm-zaminev_task_10',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='m-zaminev_task_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 16),
    catchup=False,
    tags=['m-zaminev_task_10']

) as dag:

    def push_xcom():
        return "Airflow tracks everything"

    def pull_xcom(ti):
        res = ti.xcom_pull(
            key='return_value',
            task_ids='push_xcom'
        )
        print(res)
    
    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom,
    )

    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )
    
    t1 >> t2