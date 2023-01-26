from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


with DAG(
    'hw_9_r-kulaev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_9_r-kulaev',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 27),
    catchup=False,
    tags=['hw_9_r-kulaev'],
) as dag:
    def push_xcom(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    def pull_xcom(ti):
        test = ti.xcom_pull(
            task_ids='push_xcom',
            key='sample_xcom_key'
        )
        print(test)

    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom,
    )

    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )

    t1 >> t2
